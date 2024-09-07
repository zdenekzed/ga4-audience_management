from datetime import date

import functions_framework
from google.cloud import bigquery, storage
from googleapiclient.discovery import build

analytics_admin = build("analyticsadmin", "v1alpha")
bq_client = bigquery.Client()
gcs_client = storage.Client()
gcs_bucket = "livesport-dwh-transformation-ga4-sftp-data-import"
merge_gcs_file = "data_import_audiences.csv"
property_id = "properties/153856017"


def compose_audience_body(name, description, duration, segment_value):
    body = {
        "displayName": name,
        "description": description,
        "membershipDurationDays": duration,
        "adsPersonalizationEnabled": True,
        "filterClauses": [
            {
                "clauseType": "INCLUDE",
                "simpleFilter": {
                    "scope": "AUDIENCE_FILTER_SCOPE_ACROSS_ALL_SESSIONS",
                    "filterExpression": {
                        "andGroup": {
                            "filterExpressions": [
                                {
                                    "orGroup": {
                                        "filterExpressions": [
                                            {
                                                "dimensionOrMetricFilter": {
                                                    "fieldName": "customUser:audience_segment",
                                                    "stringFilter": {
                                                        "matchType": "EXACT",
                                                        "value": segment_value,
                                                    },
                                                    "atAnyPointInTime": True,
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                },
            }
        ],
    }
    return body


def query_config():
    query = f""" 
            SELECT
              import_date,
              archive_date,
              segment_value,
              display_name,
              description,
              membership_duration_days,
              dataform_sql
            FROM
              `livesport-dwh-transformation.L2_user_journey_source.slct_audience_import`
            """
    rows = list(bq_client.query(query))
    return list(rows)


def filter_valid_config(date, current_date=date.today()):
    rows = query_config()
    valid_config = [i for i in rows if i.get(date) == current_date]
    return valid_config


def list_audiences():
    list_job = (
        analytics_admin.properties().audiences().list(parent=property_id, pageSize=200)
    )
    request = list_job.execute()
    return request


def create_ga_audience(name, description, duration, segment_value):
    body = compose_audience_body(name, description, duration, segment_value)
    creation_job = (
        analytics_admin.properties().audiences().create(parent=property_id, body=body)
    )
    request = creation_job.execute()
    print(f"created audience: {request}")


def archive_ga_audience(audience_id):
    archive_job = analytics_admin.properties().audiences().archive(name=audience_id)
    archive_job.execute()
    print(f"archived audience: {audience_id}")


def export_bq_table(table):
    project_id = "livesport-cz-analytics"
    dataset_id = "audiences"
    destination_uri = f"gs://{gcs_bucket}/{table}"
    table_id = f"{project_id}.{dataset_id}.{table}"
    extract_job = bq_client.extract_table(table_id, destination_uri)
    extract_job.result()
    print(f"extracted table: {table_id}")


def delete_gcs_blob(blob):
    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(blob)
    blob.delete()
    print(f"deleted blob: {blob.name}")


def delete_gcs_merge_blob(blob=merge_gcs_file):
    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(blob)
    if blob.exists():
        blob.delete()
        print(f"deleted merge blob: {blob.name}")
    else:
        print("merge blob doesn't exists")


def list_gcs_blobs():
    blobs = gcs_client.list_blobs(gcs_bucket)
    blobs = [i.name for i in blobs if i.name != merge_gcs_file]
    return blobs


def compose_gcs_blobs(blobs):
    bucket = gcs_client.bucket(gcs_bucket)
    destination = bucket.blob(merge_gcs_file)
    sources = [bucket.blob(i) for i in blobs]
    print(f"files to compose: {sources}")
    destination.compose(sources)


def import_data(import_date, name, description, duration, segment_value, table):
    valid_config = filter_valid_config(import_date)
    for i in valid_config:
        create_ga_audience(
            i.get(name), i.get(description), i.get(duration), i.get(segment_value)
        )
        export_bq_table(i.get(table))


def remove_data(archive_date, name, table):
    valid_config = filter_valid_config(archive_date)
    audiences_list = list_audiences().get("audiences")
    filtered_audiences = [
        {"audience_id": i.get("name"), "table_name": j.get(table)}
        for i in audiences_list
        for j in valid_config
        if i.get("displayName") == j.get(name)
    ]
    for i in filtered_audiences:
        archive_ga_audience(i.get("audience_id"))
        delete_gcs_blob(i.get("table_name"))


@functions_framework.cloud_event
def main(cloud_event):
    import_data(
        "import_date",
        "display_name",
        "description",
        "membership_duration_days",
        "segment_value",
        "dataform_sql",
    )
    remove_data("archive_date", "display_name", "dataform_sql")
    blobs = list_gcs_blobs()
    compose_gcs_blobs(blobs) if blobs else delete_gcs_merge_blob()

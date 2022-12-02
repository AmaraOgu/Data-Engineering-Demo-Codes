import argparse
import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service-account.json"

SCHEMA = {
        "fields": [
            {
                "name": 'created_at',
                "type": "TIMESTAMP",
                
            },
            {
                "name": 'edit_history_tweet_ids',
                "type": "INT64",
                "mode": "REPEATED"
            },
            {
                "name": 'id',
                "type": "INT64",
                "mode": "NULLABLE"
            },
            {
                "name": 'lang',
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": 'source',
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "text",
                "type": "STRING",
            },
        ]
    }

#Initialize argparse
parser = argparse.ArgumentParser()

#Get the project id
parser.add_argument(
    "--project_id",
    help="Your GCP project ID",
    required=True
)
#Get the inout Pub/Sub Subcription
parser.add_argument(
    "--input_subscription",
    help="Input PubSub subscription specified as: "
    "projects/YOUR_PROJECT_ID/subscriptions/PUBSUB-SUBSCRIPTION-ID.",
    required=True
)

#Get the Bigquery output table
parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        required=True
    )

known_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
pipeline_options.view_as(GoogleCloudOptions).project = known_args.project_id

# this function runs the streaming pipeline
def run(
    input_subscription: str,
    output_table: str,
):
    with beam.Pipeline(options=pipeline_options) as p:
            (
                p
                | "Read from Pub/Sub subscription" >> beam.io.ReadFromPubSub(subscription=input_subscription)
                | "Decode and parse Json" >> beam.Map(lambda element: json.loads(element.decode("utf-8")))
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    output_table,
                    schema=SCHEMA,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
                # | beam.Map(print)
            )


if __name__ == "__main__":
    run(input_subscription=known_args.input_subscription,
        output_table=known_args.output_table
        )


'''
CLI command to run code

 python3 export_to_bq.py \
    --project_id "PROJECT-ID" \
    --input_subscription "projects/PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
    --output_table  "PROJECT:DATASET.TABLE or DATASET.TABLE." 

or 

python3 export_to_bq.py \
    --project_id "PROJECT-ID" \
    --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
    --output_table  "PROJECT:DATASET.TABLE or DATASET.TABLE." \
    --runner DataflowRunner \
    --temp_location "gs://BUCKET-NAME/temp" \
    --region us-central1

'''

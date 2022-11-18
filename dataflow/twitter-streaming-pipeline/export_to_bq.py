import argparse
import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service-account.json"

input_subscription = "projects/your-project-id/subscriptions/twitter-pipeline-sub"
output_table_name = "raw_tweets"
dataset_name = "twitter_data"
raw_tweets_schema = {
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
    '--project_id',
    help='Your GCP project ID',
    required=True
)
#Get the inout Pub/Sub Subcription
parser.add_argument(
    "--input_subscription",
    help='Input PubSub subscription in the form "projects/YOUR_PROJECT_ID/subscriptions/PUBSUB-SUBSCRIPTION-ID."',
    required=True
)
#Get the Dataset name
parser.add_argument(
    "--dataset_name", 
    help="Output BigQuery Dataset name", 
    default=dataset_name
)
#Get the output BigQuery table name
parser.add_argument(
    "--output_table_name", 
    help="Output BigQuery Table name", 
    default=output_table_name
)

known_args, pipeline_args = parser.parse_known_args()

input_subscription = known_args.input_subscription
dataset_name = known_args.dataset_name
output_table_name = known_args.output_table_name
project_id = known_args.project_id

pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
pipeline_options.view_as(GoogleCloudOptions).project = project_id

# this function runs the streaming pipeline
def run():
    with beam.Pipeline(options=pipeline_options) as p:
            (
                p
                | "Read from Pub/Sub subscription" >> beam.io.ReadFromPubSub(subscription=input_subscription)
                | "Decode and parse Json" >> beam.Map(lambda element: json.loads(element.decode("utf-8")))
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    output_table_name,
                    dataset=dataset_name,
                    project=known_args.project_id,
                    schema=raw_tweets_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
                # | beam.Map(print)
            )


if __name__ == "__main__":
    run()


'''
CLI command to run code

 python3 export_to_bq.py \
    --project_id 'PROJECT-ID' \
    --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" 
or 
python3 export_to_bq.py \
    --project_id 'PROJECT-ID' \
    --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
    --output_table_name "OUTPUT-TABLE-NAME" \
    --dataset_name "DATASET-NAME" \
    --runner DataflowRunner \
    --temp_location "gs://BUCKET-NAME/temp" \
    --region us-central1
'''

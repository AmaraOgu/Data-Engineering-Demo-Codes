import argparse
import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
import time
from typing import Any, Dict

# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/sa-file.json"

SCHEMA = ",".join(
    [
        "processing_time:TIMESTAMP",
        "source:STRING",
        "tweet_count:INT64"
    ]
)

# #Initialize argparse
parser = argparse.ArgumentParser()

#Get the project id
parser.add_argument(
    '--project_id',
    help='Your GCP project ID',
    required=True
    )

#Get the input Pub/Sub Subcription
parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/PROJECT/subscriptions/SUBSCRIPTION_ID."',
        required=True
    )

#Get the Bigquery output table
parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        required=True
    )

#Get the window interval
parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )

known_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
pipeline_options.view_as(GoogleCloudOptions).project = known_args.project_id

def add_processing_time(message) -> Dict[str, Any]:
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = message
    return {
        "source": row['source'],
        "tweet_count": row['tweet_count'],
        "processing_time": int(time.time()),
    }

# this function runs the pipeline
def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int,
):
    with beam.Pipeline(options=pipeline_options) as p:
        (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | "Decode and parse Json" >> beam.Map(lambda message: json.loads(message.decode("utf-8")))
        | "Events fixed Window" >> beam.WindowInto(beam.window.FixedWindows(window_interval_sec), allowed_lateness=5)
        | "Add source keys" >> beam.WithKeys(lambda msg: msg["source"])
        | "Group by source" >> beam.GroupByKey()
        | "Get tweet count">> beam.MapTuple(
                                lambda source, messages: {
                                    "source": source,
                                    "tweet_count": len(messages),
                                }
                            )
        | "Add processing time" >> beam.Map(add_processing_time)
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                                    output_table,
                                    schema=SCHEMA,
                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                )
        )


if __name__ == "__main__":
    run(
        input_subscription=known_args.input_subscription,
        output_table=known_args.output_table,
        window_interval_sec=known_args.window_interval_sec
    )


'''
CLI command to run code

 python3 agg_and_write2bq.py \
    --project_id 'PROJECT-ID' \
    --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
    --output_table  "PROJECT:DATASET.TABLE or DATASET.TABLE." 
or 
python3 agg_and_write2bq.py \
    --project_id 'PROJECT-ID' \
    --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
    --output_table  "PROJECT:DATASET.TABLE or DATASET.TABLE." \
    --runner DataflowRunner \
    --temp_location "gs://BUCKET-NAME/temp" \
    --region us-central1
'''

# Overview

This code implements an approach to building a streaming pipeline in Google Cloud Platform (GCP) which gets data from Twitter using the Twitter API, ingests the streaming data into Google Cloud Pub/Sub, transforms the data using dataflow and applies aggregations, and then save it to BigQuery.  

The blog for this code is published on [Medium](https://medium.com/@amarachi.ogu/build-a-streaming-data-pipeline-in-gcp-bd0af69440c9)  

Pipeline Architecture Diagram 
![Pipeline Architecture Diagram](https://miro.medium.com/max/1400/0*gmeGcSQq57wpZDV7)


## Stream tweets from Twitter

```
python3 stream_tweet.py \
  --bearer_token "YOUR-BEARER-TOKEN" \
  --stream_rule "STREAM-RULE"
```

## Send tweets to Cloud Pub/Sub

```
export GOOGLE_AUTH_CREDENTIALS="path/to/sa-file.json"
```
```
python3 stream_to_pubsub.py \
  --bearer_token "YOUR-BEARER-TOKEN" \
  --stream_rule "STREAM-RULE" \
  --project_id "PROJECT-ID" \
  --topic_id "PUB/SUB-TOPIC-ID"
```

## Use Dataflow to process the data and write to BigQuery

### Using Python direct Apache Beam runner 
  
```
python3 export_to_bq.py \
  --project_id "PROJECT-ID" \
  --input_subscription "projects/PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION-ID"
```

### Using dataflow runner  
```
python3 export_to_bq.py \
  --project_id "PROJECT-ID" \
  --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION-ID" \
  --output_table_name "OUTPUT-TABLE-NAME" \
  --dataset_name "DATASET-NAME" \
  --runner DataflowRunner \
  --temp_location "gs://BUCKET-NAME/temp" \
  --region us-central1
```

## Transform the data and write to BigQuery

### Using Python direct Apache Beam runner 
  
```
python3 agg_and_write2bq.py \
  --project_id "PROJECT-ID" \
  --input_subscription "projects/PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION-ID"
```

### Using dataflow runner  
```
python3 agg_and_write2bq.py \
  --project_id "PROJECT-ID" \
  --input_subscription "projects/'PROJECT-ID/subscriptions/PUBSUB-SUBSCRIPTION" \
  --output_table_name "OUTPUT-TABLE-NAME" \
  --dataset_name "DATASET-NAME" \
  --runner DataflowRunner \
  --temp_location "gs://BUCKET-NAME/temp" \
  --region us-central1
```
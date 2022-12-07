# Overview

The pipeline used here is a streaming pipeline that reads messages from a Cloud Pub/Sub subscriber, applies a fixed window technique, performs aggregations, and writes the resulting PCollections to a BigQuery table for analytical use.


The blog for this code is published on [Medium](https://medium.com/@amarachi.ogu/deploy-a-reusable-custom-data-pipeline-using-dataflow-flex-template-in-gcp-fb8f40318bad)  

## File Layout
```
.
├── .gitignore: Specifies intentionally untracked files that Git should ignore 
├── Dockerfile: Contains specifications for the container which will package the pipeline code.  
├── README.md: A brief overview of the code.  
├── agg_and_write2bq.py: contains all the pipeline code.  
├── Metadata.json: contains Metadata about the pipeline and validation rules for its parameters.  
├── requirements.txt: Ideally should contain packages needed to be installed on the Container. Left empty in this case 
```

## To create a new repository, run the following command:
```
gcloud artifacts repositories create REPOSITORY \
    --repository-format=docker \
    --location=REGION \
    --async
```

Replace REPOSITORY and REGION with a name for your repository and region respectively. For each repository location in a project, repository names must be unique.

In this case, we’d name the repository twitter-pipeline in the us-central1 region 

```
gcloud artifacts repositories create twitter-pipeline \
    --repository-format=docker \
    --location=us-central1 \
    --async
```

## Push the image to Artifact Registry

Run the command:
```
gcloud builds submit --tag us-central1-docker.pkg.dev/PROJECT-ID/twitter-pipeline/dataflow/stream-data:latest .
```

## Create the Flex Template
Run the command:
```
cloud dataflow flex-template build gs://pipe-line-demo-bucket/dataflow/templates/streaming-pipeline \
    --image "us-central1-docker.pkg.dev/PROJECT_ID/twitter-pipeline/dataflow/stream-data:latest" \
    --sdk-language "PYTHON" \
    --metadata-file "metadata.json"
```

## Run the Flex Template pipeline

```
cloud dataflow flex-template run "twitter-pipeline`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://pipe-line-demo-bucket/dataflow/templates/streaming-pipeline" \
    --parameters input_subscription="projects/PROJECT_ID/subscriptions/twitter-pipeline-sub" \
    --parameters output_table="PROJECT_ID:twitter_data.agg_tweets" \
    --region "us-west1" \
    --service-account-email="twitter-pipeline@PROJECT_ID.iam.gserviceaccount.com"

```
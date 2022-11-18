import tweepy
from google.cloud import pubsub_v1
import json
import argparse
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service-account.json"

# This function gets the arguments that will be passed in the command-line
def parse_args():

    #Initialize argparse
    parser = argparse.ArgumentParser()

    #Get the bearer token
    parser.add_argument(
        '--bearer_token',
        help='Bearer Token gotten from your Twitter Developer account. It authenticates requests on behalf of your developer App.',
        required=True
    )

    #Get the stream rule
    parser.add_argument(
        '--stream_rule',
        help='the keyword of interest you intend to filter for',
        required=True
    )

    #Get the project id
    parser.add_argument(
        '--project_id',
        help='Your GCP project ID',
        required=True
    )

    #Get the Pub/Sub topic id
    parser.add_argument(
        '--topic_id',
        help='Your Pub/Sub topic id',
        required=True
    )

    return parser.parse_args()

#define variables
args = parse_args() 
bearer_token = args.bearer_token
stream_rule = args.stream_rule
project_id = args.project_id
topic_id = args.topic_id

# create publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def write_to_pubsub(data):
    encode_data = json.dumps(data).encode("utf-8")

    pubsub_message = publisher.publish(
        topic_path, encode_data
    )
    print(pubsub_message.result())


class MyStream(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token)

    # This function gets called after successfully connecting to the streaming API
    def on_connect(self):
        print("Connected")

    #This function gets called when a response is received 
    def on_response(self, response): 
        tweet_data = response.data.data
        
        # write the tweets to pubsub
        write_to_pubsub(tweet_data)

# Creating Stream object
stream_obj = MyStream(bearer_token=bearer_token)

def stream_tweets():

    #specify tweet fields you want to return. see documentation https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream#:~:text=each%20returned%20Tweet.-,Specify%20the%20desired%20fields%20in%20a%20comma%2Dseparated%20list%20without%20spaces%20between%20commas%20and%20fields,-.%20While%20the%20user
    tweet_fields = ['text','created_at','lang','source'] 

    # remove existing rules
    rules = stream_obj.get_rules().data
    if rules is not None:
        existing_rules = [rule.id for rule in stream_obj.get_rules().data]
        stream_obj.delete_rules(ids=existing_rules)

    # add stream rules and filter
    stream_obj.add_rules(tweepy.StreamRule(stream_rule))
    stream_obj.filter(tweet_fields=tweet_fields)   


if __name__ == "__main__":
    stream_tweets()


'''
CLI command to run this pipeline on dataflow

python3 stream_to_pubsub.py \
    --bearer_token 'YOUR-BEARER-TOKEN' \
    --stream_rule 'STREAM-RULE' \
    --project_id 'PROJECT-ID' \
    --topic_id 'PUB/SUB TOPIC ID'
'''

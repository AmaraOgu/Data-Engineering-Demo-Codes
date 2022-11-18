import tweepy
import argparse

# This function gets the arguments that will be parsed in the command-line
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

    return parser.parse_args()

args = parse_args() 
bearer_token = args.bearer_token
stream_rule = args.stream_rule

'''
StreamingClient documentation:
https://docs.tweepy.org/en/stable/streamingclient.html

Stream rule documentation:
https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule 
'''

class MyStream(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token)


    # This function gets called after successfully connecting to the streaming API
    def on_connect(self):
        print("Connected")

    #This function gets called when a response is received
    def on_response(self, response): 
        tweet_data = response.data.data
        
        print(tweet_data)

# Creating Stream object
stream_obj = MyStream(bearer_token=bearer_token)

def stream_tweets():

    #specify tweet fields you want to return. see documentation https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream#:~:text=each%20returned%20Tweet.-,Specify%20the%20desired%20fields%20in%20a%20comma%2Dseparated%20list%20without%20spaces%20between%20commas%20and%20fields,-.%20While%20the%20user
    tweet_fields = ['text','created_at','lang','source'] 
    # tweet_fields = ['source']

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
CLI command to run code

python3 stream_tweet.py \
    --bearer_token 'YOUR-BEARER-TOKEN' \
    --stream_rule 'STREAM-RULE' 

'''
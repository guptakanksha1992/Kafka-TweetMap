#----------Parsing Configuration File--------------------

myvars = {}
with open("auth.txt") as myfile:
    for line in myfile:
        name, var = line.partition(":")[::2]
        myvars[name.strip()] = var.strip()

#----------Twitter API Details---------------------------

import tweepy
import json
from tweepy import Stream
from tweepy.streaming import StreamListener


consumerKey=myvars['twitter_consumer_key']
consumerSecret=myvars['twitter_consumer_secret']
accessToken=myvars['twitter_access_token']
accessSecret=myvars['twitter_access_secret']

#----------Sentiment Analysis methods------------------

from TweetSentimentAnalysis import *

#----------SQS Details---------------------------

import boto.sqs

KEYWORDS = ['Food', 'Travel', 'Hollywood', 'Art', 'Cartoons', 'Pizza', 'Friends', 'Miami']
REQUEST_LIMIT = 420

class TweetListener(StreamListener):
    def on_data(self, data):
        try:
            parse_data(data)
        except Exception,e:
            # print(data)
            print("No location data found" + e)

        return(True)

    def on_error(self, status):
        errorMessage = "Error - Status code " + str(status)
        print(errorMessage)
        if status == REQUEST_LIMIT:
            print("Request limit reached. Trying again...")
            exit()


def formatTweet(id, location_data, tweet, author, timestamp):
    tweet = {
        "id": id,
        "message": tweet,
        "author": author,
        "timestamp": timestamp,
        "location": location_data
    }
    return tweet

def parse_data(data):
    try:
    	json_data_file = json.loads(data)
    except Exception, e:
    	print 'Parsing failed'
    	print e
    # Could be that json.loads has failed

    #print 'JSON DATA FILE:', json_data_file

    try:
        location = json_data_file["place"]
        coordinates = json_data_file["coordinates"]
    except Exception,e:
        print 'Location data parsing erroneous'
        print e

    # Setting location of the tweet

    if coordinates is not None:
        final_longitude = json_data_file["coordinates"][0]
        final_latitude = json_data_file["coordinates"][0]
    elif location is not None:
        coord_array = json_data_file["place"]["bounding_box"]["coordinates"][0]
        longitude = 0;
        latitude = 0;
        for object in coord_array:
            longitude = longitude + object[0]
            latitude = latitude + object[1]
        final_longitude = longitude / len(coord_array)
        final_latitude = latitude / len(coord_array)
    else:
    	# Insert code for random final_longitude, final_latitude here

        final_longitude=random.uniform(-180.0,180.0)
        final_latitude=random.uniform(-90.0, +90.0)
        
    tweetId = json_data_file['id_str']
    tweet = json_data_file["text"]
    author = json_data_file["user"]["name"]
    timestamp = json_data_file["created_at"]
    location_data = [final_longitude, final_latitude]

    # Tweet ready (without sentiment analysis by this point) - sending to queue
   # print tweetId, location_data, tweet, author, timestamp

    try:
        # Format tweet into correct message format for SQS
        formatted_tweet = formatTweet(tweetId, location_data, tweet, author, timestamp)
        tweet = json.dumps(formatted_tweet)
    	print 'Trying to publish to Queue the tweet', tweet
        # Establishing Connection to SQS
        conn = boto.sqs.connect_to_region("us-west-2", aws_access_key_id=myvars['aws_api_key'],
                                          aws_secret_access_key=myvars['aws_secret'])
        queue_name = conn.get_queue_by_name('tweet_queue')
        response = queue_name.send_message(MessageBody=tweet)
        print(type(response))
        print("Added tweet to SQS")

    except Exception, e:
    	print("Failed to insert tweet into SQS")
    	print str(e)


def elastic_worker_sentiment_analysis():
    # This method acts as an Elastic BeanStalk worker

    # Receiving the message from SQS
    q = conn.get_queue('tweet_queue')

    # Storing the result set
    rs = q.get_messages()

    # Extracting the message from resultset
    m = rs[0]

    # Extracting tweet from message
    tweet = m.get_body()

    sentiment = tweet_sentiment_analysis(tweet)

    # SNS Connection

    conn = boto.sns.connect_to_region( 'us-west-2', aws_access_key_id=myvars['aws_api_key'], aws_secret_access_key=myvars['aws_secret'] )
    topic = 'arn:aws:sns:us-west-2:708464623468:tweet_sentiment'

    # Appending sentiment data to JSON Format of the message tweet
    message_json =  json.loads(m)
    message_json['sentiment'] = sentiment

    # Publishing to SNS
    print conn.publish(topic=topic,message = message_json)

def startStream():
    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessSecret)
    while True:
        try:
            twitterStream = Stream(auth, TweetListener())
            twitterStream.filter(track=KEYWORDS)
        except:
            print("Restarting Stream")
            continue

    #The location specified above gets all tweets, we can then filter and store based on what we want
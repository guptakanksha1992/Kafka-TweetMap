import tweepy
import json
from tweepy import Stream
from tweepy.streaming import StreamListener
from TweetHandler import TwitterHandler
from ElasticSearchServices import ElasticSearchServices
import random
import sys
from watson_developer_cloud import NaturalLanguageUnderstandingV1
import watson_developer_cloud.natural_language_understanding.features.v1 as features

# accessSecret='lrJiIAcGZRvxPNTTvo5TCe3KRJp6FaqNZGIOC0SSOHLsx'
reload(sys)
sys.setdefaultencoding('utf8')
#----------Twitter API Details---------------------------

consumerKey='uJ8ywVGKTC7aubwomDuWrAu9t'
consumerSecret='qbcDPiGjdNGj3B2EiXja3z0ppxMenePTzp6X1nAur2CakwLF1G'
accessToken='3287103102-m7iUaz9H6eOmgl50DBMXPfePIywVFEnYldLAUoa'
accessSecret='lrJiIAcGZRvxPNTTvo5TCe3KRJp6FaqNZGIOC0SSOHLsx'

KEYWORDS = ['Food', 'Travel', 'Hollywood', 'Art', 'Cartoons', 'Pizza', 'Friends', 'Miami']
REQUEST_LIMIT = 420

#---- Elastic Search Details -------

index = "tweettrends"
collection = {
	"mappings": {
		"finaltweets2": {
			"properties": {
				"id": {
					"type": "string"
				},
                "source": {
					"type": "string"
				},
				"message": {
					"type": "string"
				},
				"author": {
					"type": "string"
				},
				"timestamp": {
					"type": "string"
				},
				"location": {
					"type": "geo_point"
				},
                "sentiment": {
					"type": "string"
				},
                "anger": {
					"type": "float"
				},
                "joy": {
					"type": "float"
				},
                "sadness": {
					"type": "float"
				},
                "fear": {
					"type": "float"
				},
                "disgust": {
					"type": "float"
				}
			}
		}
	}
}


#--------------------------------------------------------


try:
    collection_service = ElasticSearchServices()
    collection_service.create_collection(index, collection)
except:
    print "Index already created"

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

def parse_data(data):
    #print 'Data received at parse_data', data
    json_data_file = json.loads(data)
    # Could be that json.loads has failed
    tweetHandler = TwitterHandler()

    location = json_data_file["place"]
    coordinates = json_data_file["coordinates"]

    '''print 'Value of location', location
                print 'Value of coordinates', coordinates'''

    if coordinates is not None:
        # print(json_data_file["coordinates"])
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

    # sentiment analysis
    # watson username and password
    wusername = 'b9d772d4-5e8c-41ca-a571-97abf6136f61'
    wpassword = 'qUlzMi4EImig'

    natural_language_understanding = NaturalLanguageUnderstandingV1(
        version='2017-02-27',
        username=wusername,
        password=wpassword)

    def sentimentAnalysis(text):
        # encoded_text = urllib.quote(text)
        response = natural_language_understanding.analyze(
            text=text,
            features=[features.Emotion(), features.Sentiment()])
        # print text
        emotion_dict = response['emotion']['document']['emotion']
        overall_sentiment = response['sentiment']['document']['label']

        # print ("The overall sentiment of the text is: "+overall_sentiment)
        # print("The emotional quotient of the text is as follows: ")
        # for key in emotion_dict:
        #     print(key + " : " + str(emotion_dict[key]))
        return overall_sentiment, emotion_dict

    def clean_tweet(tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    cleansed_tweet = clean_tweet(tweet)
    sentimentRating, allemotions = sentimentAnalysis(cleansed_tweet)
    anger = allemotions['anger']
    joy = allemotions['joy']
    sadness = allemotions['sadness']
    fear = allemotions['fear']
    disgust = allemotions['disgust']

    print("me", tweetId, location_data, tweet, author, timestamp, sentimentRating, anger, joy, sadness, fear, disgust)
    try:
        print(
        tweetHandler.insertTweet(tweetId, location_data, tweet, author, timestamp, sentimentRating, anger, joy, sadness,
                                 fear, disgust))
    except Exception, e:
        print("Failed to insert tweet: " + str(e))



    tweetHandler.insertTweet(tweetId, location_data, tweet, author, timestamp)
    try:
    	print 'Trying to insert tweet', tweet, 'from TweetListener'
        print(tweetHandler.insertTweet(tweetId, location_data, tweet, author, timestamp))
    except:
    	print('F')
        print("Failed to insert tweet")

    print '---------------'

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


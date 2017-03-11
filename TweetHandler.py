# Here is where we can store and seach for tweets from the document

from ElasticSearchServices import ElasticSearchServices

class TwitterHandler:

	def __init__(self):
		self.es = ElasticSearchServices()
		self.index = "finaltwittermapindex5"
		self.doc_type = "finaltweets2"

	def getTweets(self, keyword):
		body = {
			"query": {
				"match": {
					"_all": keyword
				}
			}
		}

		size = 10000
		result = self.es.search(self.index, self.doc_type, body, size)

		return result

	def getTweetsWithDistance(self, keyword, distance, latitude, longitude):
		distance_string = distance + 'km'
		print 'Searching ', distance_string, ' from location Latitude: ', latitude, ' ; Longitude: ', longitude
		body = {
			"query": {
				"match": {
					"_all": keyword
				}
			},
			"filter": {
				"geo_distance": {
					"distance": distance_string,
					"distance_type": "sloppy_arc",
					"location": {
						"lat": latitude,
						"lon": longitude
					}
				}
			}
		}

		size = 10000
		result = self.es.search(self.index, body)

		return result

	def insertTweet(self, id, location_data, tweet, author, timestamp):
		#print "Inserting the follwing tweet: "
		# print id
		#print tweet
		#print author, timestamp, location_data[0], location_data[1]
		body = {
			"id": id,
			"message": tweet,
			"author": author,
			"timestamp": timestamp,
			"location": location_data
		}

		result = self.es.store_data(self.index, self.doc_type, body)

		return result


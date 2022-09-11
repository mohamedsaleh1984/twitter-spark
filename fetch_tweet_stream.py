import tweepy
import config

client = tweepy.Client(config.BEARER_TOKEN,config.API_KEY,config.API_KEY_SECRET,config.ACCESS_TOKEN,config.ACCESS_TOKEN_SECRET)

auth = tweepy.OAuth1UserHandler(config.API_KEY,config.API_KEY_SECRET,config.ACCESS_TOKEN,config.ACCESS_TOKEN_SECRET)

api = tweepy.API(auth);

search_terms = ["Hadoop","Spark","Big Data","Hive"]

class TwitterStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected to Twitter Stream...")
    
    def on_data(self, data):
        print(data)
        #Open file for append
        fileHandler = open("tweets.txt", "a")
        fileHandler.write(str(data))
        fileHandler.close()
        return True

    def on_error(self, status):
        print("An Error has Occured " + status)

stream = TwitterStream(config.BEARER_TOKEN)

for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields = ["referenced_tweets", "lang"])

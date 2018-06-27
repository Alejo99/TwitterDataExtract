import tweepy  # requires tweepy 3.6.0
import json
import operator
from urllib3 import exceptions
from time import sleep
from TweetUtils import Twutils


# StdOut listener
class StdOutListener(tweepy.StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        if len(self.state_handler.tweet_ids) < self.state_handler.max_tweets:
            json_data = json.loads(data)
            # determine if it is a retweet
            if not self.state_handler.utils.is_retweet(json_data):
                # determine if tweet has valid urls
                valid_urls = self.state_handler.utils.get_valid_urls(json_data)
                id_str = json_data['id_str']
                if len(valid_urls) > 0:
                    # backup write all data to txt file in raw json format
                    with open('streaming/streaming_data.txt', 'a+') as file:
                        file.write(json.dumps(json_data) + "\n")

                    # get formatted tweet
                    current_tweet = utils.format_tweet(json_data)
                    self.state_handler.tweet_ids.append(current_tweet[0])
                    # write tweet to csv file
                    self.state_handler.utils.append_to_csv_file('streaming/streaming_tweets.csv', current_tweet)

                    # get urls
                    tweet_urls = utils.format_urls(id_str, set([u[1] for u in valid_urls]))
                    # write urls to csv file
                    self.state_handler.utils.append_to_csv_file('streaming/streaming_tweet_urls.csv', tweet_urls, True)
                    # collect original urls
                    original_urls = [u[0] for u in valid_urls]
                    utils.add_to_dict(self.state_handler.urls, original_urls)

                    # get named entities
                    entities = set(utils.get_entities(utils.get_text(json_data)))
                    # get named entities -> tweets
                    ners_tweets = utils.format_ners(id_str, entities)
                    # write named entities to csv file
                    self.state_handler.utils.append_to_csv_file('streaming/streaming_ner.csv',
                                                  [ent.split('|', 1) for ent in entities.difference(self.state_handler.ners)],
                                                  True)
                    # write named entities -> tweet to csv file
                    self.state_handler.utils.append_to_csv_file('streaming/streaming_ner_tweet.csv', ners_tweets, True)
                    # union of entities sets to prevent duplicates in next data iteration
                    self.state_handler.ners = self.state_handler.ners.union(entities)
                else:
                    self.state_handler.emptyUrls += 1
            return True
        else:
            # limit reached, end stream
            return False

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # limit threshold, disconnect
            return False

    def __init__(self, state_handler):
        self.state_handler = state_handler
        super(StdOutListener, self).__init__()


class StateHandler:
    def __init__(self, utils, max_tweets=10):
        self.utils = utils
        self.max_tweets = max_tweets
        self.emptyUrls = 0
        self.tweet_ids = []
        self.ners = set()
        self.urls = dict()


if __name__ == '__main__':

    # OAuth Authentication
    with open("twitterauth.txt", "r") as f:
        secret = json.load(f)

    # Get the OAuth handler
    auth = tweepy.OAuthHandler(secret["Consumer Key"], secret["Consumer Secret"])
    # Set the access tokens
    auth.set_access_token(secret["Access Token Key"], secret["Access Token Secret"])

    # Utils class
    utils = Twutils(True, True)

    # State handler class
    state_handler = StateHandler(utils, 50)

    try:
        while len(state_handler.tweet_ids) < state_handler.max_tweets:
            try:
                # Build our custom listener
                listener = StdOutListener(state_handler)

                # Build Stream
                stream = tweepy.Stream(auth, listener)

                # Filter stream by language and keywords
                stream.filter(languages=["en"], track=["fifa world cup", "world cup"])
                print("Total results: ", len(state_handler.tweet_ids))
                print("Empty url results: ", str(state_handler.emptyUrls))
                print("NERs: ", str(len(state_handler.ners)))
            except exceptions.ProtocolError:
                sleep(5)
                continue
    finally:
        # close nlp pipeline
        utils.nlp.close()
        # write original urls to file
        with open('streaming/streaming_original_urls.txt', 'a+') as urlfile:
            sorted_tuples = sorted(state_handler.urls.items(), key=operator.itemgetter(1), reverse=True)
            sorted_urls = dict(sorted_tuples)
            json.dump(sorted_urls, urlfile, indent=4)

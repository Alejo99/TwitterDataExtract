import tweepy  # requires tweepy 3.6.0
import json
import operator
from TweetUtils import Twutils


if __name__ == '__main__':
    # OAuth Authentication
    with open("twitterauth.txt", "r") as f:
        secret = json.load(f)

    # Get the OAuth handler
    auth = tweepy.OAuthHandler(secret["Consumer Key"], secret["Consumer Secret"])
    # Set the access tokens
    auth.set_access_token(secret["Access Token Key"], secret["Access Token Secret"])

    # Build the API object
    api = tweepy.API(auth,
                     wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    # Utils class
    utils = Twutils(True, True)

    # Variables
    tweet_ids = set()
    empty_urls = 0
    ners = set()
    urls = dict()

    try:
        # Search an URI
        # Cursor iterates over the results
        for result in tweepy.Cursor(api.search,
                                    q='(NHS OR national health service) filter:links',
                                    lang="en",
                                    tweet_mode="extended").items(400):
            json_data = result._json
            id_str = json_data['id_str']
            # determine if it is a retweet
            if not utils.is_retweet(json_data):
                # determine if it has valid urls
                valid_urls = utils.get_valid_urls(json_data)
                if len(valid_urls) > 0:
                    # backup write all data to txt file in raw json format
                    with open('search/search_data.txt', 'a+') as file:
                        file.write(json.dumps(json_data) + "\n")

                    # get formatted tweet
                    current_tweet = utils.format_tweet(json_data)
                    tweet_ids.add(current_tweet[0])
                    # write tweet to csv file
                    utils.append_to_csv_file('search/search_tweets.csv', current_tweet)

                    # get urls
                    tweet_urls = utils.format_urls(id_str, set([u[1] for u in valid_urls]))
                    # write urls to csv file
                    utils.append_to_csv_file('search/search_tweet_urls.csv', tweet_urls, True)
                    # collect original urls
                    original_urls = [u[0] for u in valid_urls]
                    utils.add_to_dict(urls, original_urls)

                    # get named entities
                    entities = utils.get_entities(utils.get_text(json_data))
                    # get named entities -> tweets
                    ners_tweets = utils.format_ners(id_str, entities)
                    # write named entities to csv file
                    utils.append_to_csv_file('search/search_ner.csv',
                                             [ent.split('|', 1) for ent in entities.difference(ners)],
                                             True)
                    # write named entities -> tweet to csv file
                    utils.append_to_csv_file('search/search_ner_tweet.csv', ners_tweets, True)
                    # union of entities sets to prevent duplicates in next data iteration
                    ners = ners.union(entities)
                else:
                    empty_urls += 1

        # print results
        print("Total results: ", len(tweet_ids))
        print("Empty url results: ", str(empty_urls))
        print("NERs: ", str(len(ners)))
        print("Valid URLs: ", str(len(urls)))
    finally:
        # close nlp pipeline
        utils.nlp.close()
        # write original urls to file
        with open('search/search_original_urls.txt', 'a+') as urlfile:
            sorted_tuples = sorted(urls.items(), key=operator.itemgetter(1), reverse=True)
            sorted_urls = dict(sorted_tuples)
            json.dump(sorted_urls, urlfile, indent=4)

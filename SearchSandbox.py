from TwitterAPI import TwitterAPI, TwitterPager, TwitterError  # requires TwitterAPI v2.5.0
import json

if __name__ == '__main__':
    # OAuth Authentication
    with open("twitterauth.txt", "r") as f:
        secret = json.load(f)

    # Get API wrapper
    api = TwitterAPI(secret["Consumer Key"],
                     secret["Consumer Secret"],
                     secret["Access Token Key"],
                     secret["Access Token Secret"])

    # Assemble request with paging support
    # Dev environment endpoint: https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json
    req = TwitterPager(api,
                       "tweets/search/fullarchive/:dev",
                       {"query": "(game of thrones OR khaleesi OR jon snow) has:links lang:en", "maxResults": 10})

    # Get <maxResults> items every <3> seconds
    count = 0
    try:
        for item in req.get_iterator(wait=3):
            if "text" in item:
                count += 1
                print(str(count))
                print(item)
            elif 'message' in item:
                # something needs to be fixed before re-connecting
                raise Exception(item['message'])
    except TwitterError.TwitterRequestError as e:
        print("Error")
        print(e)

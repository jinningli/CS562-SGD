import json
import os
import requests
import time

class TimelineRequest:
    def __init__(self, bearer_token):

        self.bearer_token = bearer_token

        self.TWEET_FIELDS = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at',
                             'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'possibly_sensitive',
                             'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        self.USER_FIELDS = ['created_at', 'description', 'entities', 'id', 'location', 'name',
                            'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics',
                            'url', 'username', 'verified', 'withheld']
        self.MEDIA_FIELDS = ["preview_image_url", "type", "url", 'width', 'public_metrics',
                             'non_public_metrics', 'organic_metrics', 'promoted_metrics', 'alt_text']
        self.EXPANSIONS = ["attachments.poll_ids", "attachments.media_keys", "author_id",
                           "entities.mentions.username", "geo.place_id", "in_reply_to_user_id",
                           "referenced_tweets.id", "referenced_tweets.id.author_id"]
        # https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/api-reference/get-users-id-tweets

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = "Bearer {}".format(self.bearer_token)
        r.headers["User-Agent"] = "v2UserTweetsPython"
        return r

    def connect_to_endpoint(self, url, params):
        try:
            response = requests.get(url, auth=self.bearer_oauth, params=params)
            response_code = response.status_code
            text = response.text
            return_json = response.json()
        except:
            response_code = -1
            text = "HTTP Failed to establish a new connection"
            return_json = None
        if response_code != 200:
            print("Error: {}".format(text))
        return return_json, response_code

    def search_tweets_once(self, user_id, count=10,
                           next_token=None,
                           start_time=None, end_time=None,
                           verbose=False, lang=None):
        url = "https://api.twitter.com/2/users/{}/tweets".format(user_id)

        params = {"user.fields": ",".join(self.USER_FIELDS),
                  "tweet.fields": ",".join(self.TWEET_FIELDS),
                  "media.fields": ",".join(self.MEDIA_FIELDS),
                  # 'start_time': "2017-01-01T00:00:00.000Z",
                  # 'end_time': "2017-09-30T00:00:00.000Z",
                  "max_results": count,
                  "exclude": "retweets",
                  "expansions": ",".join(self.EXPANSIONS)
                  }

        if next_token is not None:
            params["pagination_token"] = next_token
        if start_time is not None:
           params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        json_response, response_code = self.connect_to_endpoint(url, params)
        if response_code != 200:
            return None
        if verbose:
            print(json.dumps(json_response, indent=4, sort_keys=True))
        return json_response

    def search_tweets(self, user_id, max_count, save_path="./data.jsonl",
                      iter_count=10,
                      start_time=None, end_time=None,
                      verbose=False, lang=None):

        print("Start searching {} start_time: {} end_time: {} max: {} lang:{}".format(
            user_id, start_time, end_time, max_count, lang))

        total_count = 0
        next_token = None

        zero_gain_iter = 0
        EARLY_STOP_THRES = 20
        MAX_RETRY = 200
        retry_count = 0

        with open(save_path, "a+", encoding="utf-8") as fout:
            while total_count < max_count:

                json_response = self.search_tweets_once(user_id=user_id, count=iter_count, next_token=next_token,
                                                        start_time=start_time, end_time=end_time, verbose=verbose,
                                                        lang=lang)

                if json_response is None:
                    print("Response Error. Sleep and retry...")
                    retry_count += 1
                    if retry_count > MAX_RETRY:
                        break
                    time.sleep(30)
                    continue
                retry_count = 0

                if "next_token" not in json_response["meta"].keys():
                    print("No next_token. All available data collected. End.")
                    break
                else:
                    next_token = json_response["meta"]["next_token"]

                result_count = json_response["meta"]["result_count"]
                zero_gain_iter = zero_gain_iter + 1 if result_count == 0 else 0
                if zero_gain_iter > EARLY_STOP_THRES:
                    print("Early Stop because zero gain iter > {}".format(EARLY_STOP_THRES))
                    break

                total_count += result_count
                fout.write(json.dumps(json_response, sort_keys=True) + "\n")
                print("Total data collected: {}, sleep for 1 second...".format(total_count))
                time.sleep(1)

        if total_count == 0:
            os.remove(save_path)
            print()
        else:
            print("[Summary] Total: {}".format(total_count))
            print()

class TimelineRequestSearch(TimelineRequest):
    def search_tweets_once(self, user_id, count=10,
                           next_token=None,
                           start_time=None, end_time=None,
                           verbose=False, lang=None):
        url = "https://api.twitter.com/2/tweets/search/all"

        params = {'query': "(from:{}) -is:retweet".format(user_id),
                  "user.fields": ",".join(self.USER_FIELDS),
                  "tweet.fields": ",".join(self.TWEET_FIELDS),
                  "media.fields": ",".join(self.MEDIA_FIELDS),
                  # 'start_time': "2017-01-01T00:00:00.000Z",
                  # 'end_time': "2017-09-30T00:00:00.000Z",
                  "max_results": count,
                  "expansions": "author_id,attachments.media_keys,in_reply_to_user_id,referenced_tweets.id"}
        if next_token is not None:
            params["next_token"] = next_token
        if lang is not None:
            params["query"] += " lang:{}".format(lang)
        if start_time is not None:
           params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time,
        json_response, response_code = self.connect_to_endpoint(url, params)
        if response_code != 200:
            return None
        if verbose:
            print(json.dumps(json_response, indent=4, sort_keys=True))
        return json_response

if __name__ == "__main__":
    # BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAeMVgEAAAAA1wuhGUm7le9L2nwEx%2FObp968TcE%3DiqOcTJ2pTtv826PLF7p93HlrhHf7VFILDp0YVLCTnzSR5YCKPJ"
    # timeline_requester = TimelineRequestSearch(BEARER_TOKEN)
    #
    # # os.makedirs("./data_new2/CNN", exist_ok=True)
    # # timeline_requester.search_tweets(428333, 100000, "./data_new2/CNN/data.jsonl", 100, lang="en",
    # #                                  start_time="2017-01-01T00:00:00.000Z",
    # #                                  end_time="2019-01-01T00:00:00.000Z")  # CNN breaking news
    #
    # os.makedirs("./data_new2/BBC", exist_ok=True)
    # timeline_requester.search_tweets("BBCBreaking", 100000, "./data_new2/BBC/data.jsonl", 100, lang="en",
    #                                  start_time="2017-01-01T00:00:00.000Z",
    #                                  end_time="2019-01-01T00:00:00.000Z")

    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAD3rVgEAAAAA%2BfTgUh8mks7zgn8XF%2F9v935xbtE%3DiaChwBh7fwQk3zhZIdoCUAdepvpxU9gISLhJbqpATkHUVxYLSR"
    timeline_requester = TimelineRequestSearch(BEARER_TOKEN)

    os.makedirs("./data_new2/CDC", exist_ok=True)
    timeline_requester.search_tweets("CDCgov", 100000, "./data_new2/CDC/data.jsonl", 100, lang="en",
                                     start_time="2021-01-01T00:00:00.000Z",
                                     end_time="2022-01-01T00:00:00.000Z")
import json
import requests
import time
import os

class TweetRetriever:
    def __init__(self, bearer_token):

        self.bearer_token = bearer_token

        self.UNUSED_TWEET_FIELDS = ['organic_metrics', 'non_public_metrics', 'promoted_metrics']
        self.TWEET_FIELDS = ['attachments', 'author_id',
                             # 'context_annotations',
                             'conversation_id', 'created_at',
                             # 'entities',
                             'geo', 'id', 'in_reply_to_user_id', 'lang', 'possibly_sensitive',
                             'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        self.USER_FIELDS = ['created_at', 'description', 'entities', 'id', 'location', 'name',
                            'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics',
                            'url', 'username', 'verified', 'withheld']
        # self.MEDIA_FIELDS = ["preview_image_url", "type", "url", 'width', 'public_metrics',
        #                      'non_public_metrics', 'organic_metrics', 'promoted_metrics', 'alt_text']
        self.MEDIA_FIELDS = []
        # https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all

    def bearerh_full_search(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2FullArchiveSearchPython"
        return r

    def connect_to_endpoint_full_search(self, url, params):
        try:
            response = requests.get(url, auth=self.bearerh_full_search, params=params)
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

    def search_tweets(self, hashtag_str, max_count, save_path="./data.jsonl",
                      lang=None, iter_count=10,
                      start_time=None, end_time=None, sort_order=None,
                      has_images=False, tag="",
                      verbose=False):

        # assert iter_count <= 100

        print("Start searching {} start_time: {} end_time: {} max: {} image:{}".format(
            hashtag_str, start_time, end_time, max_count, has_images))

        total_count = 0
        next_token = None

        zero_gain_iter = 0
        EARLY_STOP_THRES = 20
        MAX_RETRY = 200
        retry_count = 0

        with open(save_path, "a+", encoding="utf-8") as fout:
            while total_count < max_count:

                if len(hashtag_str) > 128:
                    print("Request query should be shorter than 128. Closed query: {}".format(hashtag_str))
                    break

                json_response = self.search_tweets_once(hashtag_str, iter_count,
                                                        lang, next_token,
                                                        start_time, end_time, sort_order,
                                                        has_images, tag,
                                                        verbose)

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

    def build_query_str_or(self, hashtags):
        return "(" + " OR ".join(hashtags) + ")"

    def build_query_str_and(self, hashtags):
        return "(" + " ".join(hashtags) + ")"

    def search_tweets_once(self, hashtag_str, count=10,
                           lang=None, next_token=None,
                           start_time=None, end_time=None, sort_order=None,
                           has_images=False, tag="",
                           verbose=False):
        url = f"https://api.twitter.com/2/tweets/search/all"

        # "https://developer.twitter.com/en/docs/twitter-api/v1/rules-and-filtering/search-operators"
        # https://developer.twitter.com/en/docs/tutorials/getting-historical-tweets-using-the-full-archive-search-endpoint
        #has:media
        #has:images
        #has:geo
        # lang:fr
        # has:links lang:fr
        # TWEET_FIELDS = ['attachments', 'created_at',
        #                 'geo', 'id', 'lang','source', 'text',]
        # "2017-01-01T00:00:00.000Z"

        params = {'query': f"{hashtag_str} {tag}",
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
        if sort_order is not None:
            params["sort_order"] = sort_order
        if start_time is not None:
           params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time,
        if has_images:
            params["query"] += " has:images"
        json_response, response_code = self.connect_to_endpoint_full_search(url, params)
        if response_code != 200:
            return None
        if verbose:
            print(json.dumps(json_response, indent=4, sort_keys=True))
        return json_response

if __name__ == "__main__":
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAeMVgEAAAAA1wuhGUm7le9L2nwEx%2FObp968TcE%3DiqOcTJ2pTtv826PLF7p93HlrhHf7VFILDp0YVLCTnzSR5YCKPJ"
    retriever = TweetRetriever(BEARER_TOKEN)
    # hashtags = ['#presidentielle2017', '#legislatives2017', '#politique', '#republique', '#élections', '#président',
    #             '#assembléenationale',
    #             "#macron", "#lepen", "#marinelepen", "#melenchon", "#fillon", "#LFI", "#franceinsoumise", "#enmarche",
    #             "#lrem", "#larem", "#fn", "#frontnational", "#républicains", "lr"]
    hashtags = ["http://www.cnn.com/2017/09/26/health/std-highest-ever-reported-cdc/index.html".replace("http://", "")]
    retriever.search_tweets(
        hashtag_str=retriever.build_query_str_or(hashtags),
        max_count=50,
        lang="en"
    )


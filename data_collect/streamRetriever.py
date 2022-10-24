import json
import requests
import time
import os

# NOTWORKING

class StreamRetriever:
    def __init__(self, bearer_token):

        self.bearer_token = bearer_token

        self.UNUSED_TWEET_FIELDS = ['organic_metrics', 'non_public_metrics', 'promoted_metrics']
        self.TWEET_FIELDS = ['attachments', 'author_id', 'context_annotations', 'conversation_id', 'created_at',
                             'entities', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'possibly_sensitive',
                             'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
        self.USER_FIELDS = ['created_at', 'description', 'entities', 'id', 'location', 'name',
                            'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics',
                            'url', 'username', 'verified', 'withheld']
        self.MEDIA_FIELDS = ["preview_image_url", "type", "url", 'width', 'public_metrics',
                             'non_public_metrics', 'organic_metrics', 'promoted_metrics', 'alt_text']
        # https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all


    def bearerh_stream_search(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = "Bearer {}".format(self.bearer_token)
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    def connect_to_endpoint_full_search(self, url):
        # params = {"user.fields": ",".join(self.USER_FIELDS),
        #           "tweet.fields": ",".join(self.TWEET_FIELDS),
        #           "media.fields": ",".join(self.MEDIA_FIELDS),
        #           "expansions": "author_id,attachments.media_keys,in_reply_to_user_id,referenced_tweets.id"}
        try:
            response = requests.get(url, auth=self.bearerh_stream_search, stream=True)
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

    def get_rules(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearerh_stream_search
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))
        return response.json()

    def delete_all_rules(self, rules):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearerh_stream_search,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print(json.dumps(response.json()))

    def set_rules(self, rules):
        payload = {"add": rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearerh_stream_search,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print(json.dumps(response.json()))

    # sample_rules = [
    #     {"value": "dog has:images", "tag": "dog pictures"},
    #     {"value": "cat has:images -grumpy", "tag": "cat pictures"},
    # ]

    def build_rules(self, hashtag_str, count=1000, lang=None, next_token=None, has_images=False):
        # params = {'query': f"{hashtag_str} -is:retweet",
        # params = {'query': f"{hashtag_str}",
        #           "user.fields": ",".join(self.USER_FIELDS),
        #           "tweet.fields": ",".join(self.TWEET_FIELDS),
        #           "media.fields": ",".join(self.MEDIA_FIELDS),
        #           "max_results": count,
        #           "expansions": "author_id,attachments.media_keys,in_reply_to_user_id,referenced_tweets.id"}
        # if next_token is not None:
        #     params["next_token"] = next_token
        rule_str = hashtag_str
        if lang is not None:
            rule_str += " lang:{}".format(lang)
        if has_images:
            rule_str += " has:images"
        rule = [{"value": rule_str, "tag": "StreamRetriever:{}".format(hashtag_str)}]
        return rule

    def search_tweets(self, hashtag_str, max_count, iter_count, save_path="./data.jsonl", lang=None, has_images=False):
        old_rules = self.get_rules()
        self.delete_all_rules(old_rules)
        new_rules = self.build_rules(hashtag_str, iter_count, lang=lang, has_images=has_images)
        self.set_rules(new_rules)

        total_count = 0
        MAX_RETRY = 500
        retry_count = 0

        with open(save_path, "a+", encoding="utf-8") as fout:
            while total_count < max_count:
                if len(hashtag_str) > 128:
                    print("Request query should be shorter than 128. Closed query: {}".format(hashtag_str))
                    break
                print("aaa")
                json_response = self.search_tweets_once()
                print(json_response)
                print("bb")
                if json_response is None:
                    print("Response Error. Sleep and retry...")
                    retry_count += 1
                    if retry_count > MAX_RETRY:
                        break
                    time.sleep(30)
                    continue
                retry_count = 0

                result_count = json_response["meta"]["result_count"]

                total_count += result_count
                fout.write(json.dumps(json_response, sort_keys=True) + "\n")
                print("Total data collected: {}, sleep for 1 second...".format(total_count))
                time.sleep(1)

    def search_tweets_once(self, verbose=True):
        url = "https://api.twitter.com/2/tweets/search/stream"
        json_response, response_code = self.connect_to_endpoint_full_search(url)
        if response_code != 200:
            return None
        if verbose:
            print(json.dumps(json_response, indent=4, sort_keys=True))
        return json_response

    def build_query_str_or(self, hashtags):
        return "(" + " OR ".join(hashtags) + ")"

    def build_query_str_and(self, hashtags):
        return "(" + " ".join(hashtags) + ")"

if __name__ == "__main__":
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAALe5TwEAAAAAEEmZC9aS5nW61gQd3%2F9e2csVWek%3Dd0W7NPDEUnYpg7tX127yAQau4MToT9PjpzvK6CxKK8F1vspZgR"
    retriever = StreamRetriever(BEARER_TOKEN)
    hashtags = ['Russia', 'Ukraine']
    # hashtags = ['#presidentielle2017', '#legislatives2017', '#politique', '#republique', '#élections', '#président',
    #             '#assembléenationale',
    #             "#macron", "#lepen", "#marinelepen", "#melenchon", "#fillon", "#LFI", "#franceinsoumise", "#enmarche",
    #             "#lrem", "#larem", "#fn", "#frontnational", "#républicains", "lr"]
    # hashtags = ["http://www.cnn.com/2017/09/26/health/std-highest-ever-reported-cdc/index.html".replace("http://", "")]
    retriever.search_tweets(
        hashtag_str=retriever.build_query_str_or(hashtags),
        max_count=1000,
        iter_count=100,
        save_path="./data.jsonl"
    )


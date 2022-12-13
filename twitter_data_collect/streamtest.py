import requests
import os
import json
import time

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = "YOURTOKEN"

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules():
    # You can adjust the rules if needed
    sample_rules = [
        # {"value": "dog has:images", "tag": "dog pictures"},
        {"value": "(Russia OR Ukraine OR Russian OR Ukrainian)", "tag": "keyword: russia/n/ukraine/an"},
        # {"value": "cat has:images -grumpy", "tag": "cat pictures"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()["meta"]["sent"]


def get_file_size(path):
    return os.path.getsize(path) / (1024*1024.0)


def get_stream(output_path):
    TWEET_FIELDS = ['attachments', 'author_id',
                    # 'context_annotations',
                    'conversation_id', 'created_at',
                    # 'entities',
                    'geo', 'id', 'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text', 'withheld']
    USER_FIELDS = ['created_at', 'description',
                   # 'entities',
                   'id', 'location', 'name', 'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics', 'url', 'username', 'verified', 'withheld']
    MEDIA_FIELDS = ["preview_image_url", "type", "url", 'width', 'public_metrics',
                         'non_public_metrics', 'organic_metrics', 'promoted_metrics', 'alt_text']
    # EXPANSIONS = ["attachments.poll_ids", "attachments.media_keys", "author_id",
    #                    "entities.mentions.username", "geo.place_id", "in_reply_to_user_id",
    #                    "referenced_tweets.id", "referenced_tweets.id.author_id"]
    params = {"user.fields": ",".join(USER_FIELDS),
              "tweet.fields": ",".join(TWEET_FIELDS),
              "media.fields": ",".join(MEDIA_FIELDS),
              # 'start_time': "2017-01-01T00:00:00.000Z",
              # 'end_time': "2017-09-30T00:00:00.000Z",
              # "max_results": count,
              # "exclude": "retweets",
              # "expansions": ",".join(EXPANSIONS)
              }

    start_time = time.time()
    count = 0

    with open(output_path, "a+", encoding="utf-8") as fout:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, params=params, stream=True,
        )
        print("Response code: {}".format(response.status_code))
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                fout.write(json.dumps(json_response, sort_keys=True) + "\n")
                count += 1
                if count % 1000 == 0:
                    file_size = get_file_size(output_path)
                    print("Collected: {}. Hours Spent: {:.3f}, Speed: {:.3f}/hour, File size MB: {:.3f}, File speed MB/hour: {:.3f}".format(
                        count, (time.time()-start_time)/60/60, count/((time.time()-start_time)/60/60),
                        file_size, file_size/((time.time()-start_time)/60/60)
                    ))
                if count % 100 == 0:
                    time.sleep(30)

def main():
    # os.removedirs("./ukraine_data")
    os.makedirs("./ukraine_data", exist_ok=True)
    rules = get_rules()
    delete_all_rules(rules)
    set_time = set_rules()
    print(set_time)
    time_tag = time.strftime("%Y%m%d%H%M%S_", time.localtime()) + str(time.time()).split(".")[1]
    while True:
        try:
            get_stream(output_path="./ukraine_data/data_rule_time_{}_collect_time_{}.jsonl".format(set_time, time_tag))
        except:
            print("error")


if __name__ == "__main__":
    main()

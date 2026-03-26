import csv
import json
import time
from dataclasses import dataclass

import pandas as pd
import requests


@dataclass
class api_agent:
    """
    Class object for making various reddit API requests.
    ----------
    paramaters
    ----------
    api_credentials: dictionary containing your api application credentials...
        {
            "user_agent" : "user_agent",
            "user_pass" : "user_pass",
            "client_id" : "client_id",
            "client_secret" : "client_secret",
            "application_name" : "application_name"
        }
    """

    def __init__(self, api_credentials):
        self.user_agent = api_credentials["user_agent"]
        self.user_pass = api_credentials["user_pass"]
        self.client_id = api_credentials["client_id"]
        self.client_secret = api_credentials["client_secret"]
        self.application_name = api_credentials["application_name"]

    def renew_auth_token(self):
        """
        Renew Reddit API O-Auth
        """

        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {
            "grant_type": "password",
            "username": self.user_agent,
            "password": self.user_pass,
        }
        self.headers = {"User-Agent": "{}/0.0.0".format(self.application_name)}
        request = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data=data,
            headers=self.headers,
        )
        TOKEN = request.json()["access_token"]
        self.headers["Authorization"] = f"bearer {TOKEN}"

    def get_top_subreddits(self, payload):
        def hit():
            try:
                r = requests.get(
                    "https://oauth.reddit.com/subreddits/new",
                    headers=self.headers,
                    params=payload,
                )
                status = r.status_code
                print("> http response is:", status)
            except:
                r = ""
                status = " NO HANDSHAKE "
                print("> http response is:", status)
            return r, status

        retry = 0
        while True:
            retry += 1
            r, status = hit()
            if status == 200:
                break
            time.sleep(5 * retry)
            print(" >> rety_#: {}".format(retry))
            if retry % 3 == 0:
                self.renew_auth_token()
                print("\nAUTH_RENEWED\n")
        subreddits = json.loads(r.text, strict=False)
        return subreddits

    def make_subreddits(self, path="subreddit_list.csv"):
        self.renew_auth_token()
        self.subreddits_path = path
        with open(self.subreddits_path, "w", newline="", encoding="utf-8") as _red_list:
            _red_writer = csv.writer(_red_list, delimiter=",")
            _headers = ["subreddit", "num_subscribers", "creation_utc", "nsfw_bool"]
            _red_writer.writerow(_headers)
            _payload = {"limit": "100"}
            _subreddits = self.get_top_subreddits(_payload)
            _before = ""
            while len(_subreddits) > 0:
                for key in _subreddits["data"]["children"]:
                    try:
                        _title = str(key["data"]["title"])
                    except:
                        _title = "NA"
                    try:
                        _subscribers = str(key["data"]["subscribers"])
                    except:
                        _subscribers = "NA"
                    try:
                        _created_utc = str(key["data"]["created_utc"])
                    except:
                        _created_utc = "NA"
                    try:
                        _over18 = str(key["data"]["over18"])
                    except:
                        _over18 = "NA"
                    _after = key["data"]["name"]
                    _rho = [_title, _subscribers, _created_utc, _over18]
                    _red_writer.writerow(_rho)
                    _payload = {"limit": "100", "after": _after}
                if _before == _after:
                    break
                else:
                    print("   >>> after : {}".format(_after))
                    _subreddits = self.get_top_subreddits(_payload)
                    _before = _after
        self.subreddits_df = pd.read_csv(self.subreddits_path, low_memory=False)
        self.subreddits_df = self.subreddits_df.sort_values(
            by="num_subscribers", ascending=False
        )
        self.subreddits_df.to_csv(
            path_or_buf=self.subreddits_path, sep=",", na_rep="nan", index="subreddit"
        )


__all__ = ["api_agent"]

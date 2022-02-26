"""
Witten by: Cason Konzer

pushshift_python is a wrapper for reddit community analytics. 

read the docs at: https://github.com/casonk/pushshift_python/blob/master/documentation.ipynb
"""

# Import relative libraries.
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from dateutil.relativedelta import relativedelta
from sklearn.feature_selection import RFE
from sklearn.svm import SVC
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import recall_score
from sklearn.metrics import roc_curve
from sklearn.metrics import f1_score
from sklearn.metrics import SCORERS
from sklearn.metrics import auc
from sklearn.utils import resample
from dataclasses import dataclass
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from zlib import crc32
import networkx as nx
import seaborn as sns
import pandas as pd
import numpy as np
import zstandard
import requests
import time
import math
import json
import csv
import sys
import re
import os

# Setup default plotter.
plt.style.use("dark_background")
plt.rcParams["figure.figsize"] = [16, 9]
plt.rcParams.update({"font.size": 18})
plt.rcParams.update({"text.usetex": True})

# Create global column numeric scaling function.
def numeric_scaler(col, df):
    col_max = df[col].max()
    col_min = df[col].min()
    col_range = col_max - col_min
    scaled = df[col].apply(lambda x: (x - col_min) / col_range)
    return scaled


# Create global column boolean scaling function.
def boolean_scaler(col, df):
    scaled = df[col].copy()
    scaled.replace(True, 1, inplace=True)
    scaled.replace(False, 1, inplace=True)
    return scaled


# Create global column string hasing function.
def string_scaler(col, df):
    def string_hasher(string):
        s = str(string).encode("utf-8")
        try:
            unscaled_hash = float(crc32(s) & 0xFFFFFFFF)
        except:
            unscaled_hash = np.nan()
        string_hash = unscaled_hash / 2 ** 32
        return string_hash

    scaled = df[col].apply(lambda x: string_hasher(x))
    return scaled


# Create object for direct reddit api queries.
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
        self.headers = {"User-Agent": "{}}/0.0.0".format(self.application_name)}
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


# Create query superclass
@dataclass
class query:
    """
    SuperClass for compiling reddit queries.
    ----------
    paramaters
    ----------
    query_type:
        subreddit- query provided subreddit.
        keyword- query all subreddits for provided keyword.
    query: provided subreddit or keyword.
    time_range: dictionary input {'before' : latest post time, 'after' : earliest post time}
        times can be given in unix epoch timestamp or datetime format.
    time_format:
        'unix'- defaults to unix epoch timestamp.
        'datetime'- set this option is specifing time_range in datetime format.
    post_type: selection to query for comments or submissions, defaults to both.
        'comment'- only query comments.
        'submission'- only query submission.
        defaults to query both comments and submissions.
    """

    def __init__(
        self, query_type, query, time_range, time_format="unix", post_type=None
    ):
        """
        Initilization of query object.
        """

        self.type = query_type.lower()
        self.query = query.lower()
        if time_format == "datetime":
            time_range["before"] = int(
                datetime.timestamp(datetime.strptime(time_range["before"], "%Y-%m-%d"))
            )
            time_range["after"] = int(
                datetime.timestamp(datetime.strptime(time_range["after"], "%Y-%m-%d"))
            )
        self.before = int(time_range["before"])
        self.before_dt = datetime.fromtimestamp(self.before)
        self.after = int(time_range["after"])
        self.after_dt = datetime.fromtimestamp(self.after)
        try:
            self.post_type = post_type.lower()
        except:
            self.post_type = post_type

    def create_common_data(self, p_type_, post):
        """
        Helper function to collect values common between both comments and submissions.
        """

        try:
            subreddit = post["subreddit"]
            post_id = post["id"]
            try:
                parent_id = post["parent_id"]
            except KeyError:
                parent_id = "nan"
            try:
                link_id = post["link_id"]
            except KeyError:
                link_id = "nan"
            try:
                url = post["url"]
            except KeyError:
                url = "nan"
            try:
                permalink = post["permalink"]
            except:
                permalink = "nan"
            created_utc = post["created_utc"]
            t = datetime.fromtimestamp(created_utc)
            date = t.strftime("%m/%d/%Y")
            score = post["score"]
            try:
                upvote_ratio = post["upvote_ratio"]
            except KeyError:
                upvote_ratio = "nan"
            try:
                num_comments = post["num_comments"]
            except KeyError:
                num_comments = "nan"
            try:
                controversiality = post["controversiality"]
            except:
                controversiality = "nan"
            try:
                total_awards_received = post["total_awards_received"]
            except:
                total_awards_received = "nan"
            try:
                stickied = post["stickied"]
            except:
                stickied = "nan"
            try:
                post_hint = post["post_hint"]
            except:
                post_hint = "nan"
            try:
                is_self = post["is_self"]
            except KeyError:
                is_self = "nan"
            try:
                is_video = post["is_video"]
            except KeyError:
                is_video = "nan"
            try:
                title = post["title"]
                title = r"{}".format(title)
            except KeyError:
                title = "nan"
            author = post["author"]
            author = r"{}".format(author)
            try:
                author_premium = post["author_premium"]
            except:
                author_premium = "nan"
            if p_type_ == "comment":
                try:
                    body = post["body"]
                    body = r"{}".format(body)
                except KeyError:
                    body = "nan"
            elif p_type_ == "submission":
                try:
                    body = post["selftext"]
                    body = r"{}".format(body)
                except KeyError:
                    body = "nan"
            post_type = p_type_
            post_data = {
                "post_type": post_type,
                "subreddit": subreddit,
                "id": post_id,
                "parent_id": parent_id,
                "link_id": link_id,
                "url": url,
                "permalink": permalink,
                "created_utc": created_utc,
                "datetime": date,
                "score": score,
                "upvote_ratio": upvote_ratio,
                "num_comments": num_comments,
                "controversiality": controversiality,
                "total_awards_received": total_awards_received,
                "stickied": stickied,
                "post_hint": post_hint,
                "is_self": is_self,
                "is_video": is_video,
                "title": title,
                "body": body,
                "author": author,
                "author_premium": author_premium,
            }
            return post_data
        except KeyboardInterrupt:
            pass


# Create pushshift file query object
@dataclass
class pushshift_file_query(query):
    """
    Class for compiling pushshift file queries.
    Respective files can be downloaded from : https://files.pushshift.io/reddit/
    ----------
    paramaters
    ----------
    query_type:
        'subreddit'- query provided subreddit.
        'keyword'- query all subreddits for provided keyword.
    query: provided subreddit or keyword.
    time_range: dictionary input {'before' : latest post time, 'after' : earliest post time}
        times can be given in unix epoch timestamp or datetime format.
    time_format:
        'unix'- defaults to unix epoch timestamp.
        'datetime'- set this option is specifing time_range in datetime format.
    post_type: selection to query for comments or submissions, defaults to both.
        'comment'- only query comments.
        'submission'- only query submission.
        defaults to query both comments and submissions.
    """

    def __init__(
        self, query_type, query, time_range, time_format="unix", post_type=None
    ):
        """
        Initilization of query object.
        """

        super().__init__(query_type, query, time_range, time_format, post_type)
        self.submission_folder_path = Path(
            "F:/Research/Funded/Ethical_Reccomendations/Python/Push_File/RS/2019+/"
        )
        self.comment_folder_path = Path(
            "F:/Research/Funded/Ethical_Reccomendations/Python/Push_File/RC/2019+/"
        )
        self.line_counter = 0
        self.post_counter = 0
        self.file_counter = 0
        self.errors = 0

    def set_parent_folders(self, submission_folder_path, comment_folder_path):
        """
        Set paths to pushshift files.
        """

        self.submission_folder_path = Path(submission_folder_path)
        self.comment_folder_path = Path(comment_folder_path)

    def read_lines_zst(self):
        """
        Helper function for reading from ztandandard compressed ndjson files.
        """

        with open(self.working_file, "rb") as file_handle:
            buffer = ""
            reader = zstandard.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(
                file_handle
            )
            while True:
                chunk = reader.read(2 ** 27).decode()
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")

                for line in lines[:-1]:
                    yield line, file_handle.tell()

                buffer = lines[-1]
            reader.close()

    def make_query(self, oversized=False):
        """
        Initialize the query.
        """

        self.oversized = oversized
        self.headers = [
            "post_type",
            "subreddit",
            "id",
            "parent_id",
            "link_id",
            "url",
            "permalink",
            "created_utc",
            "datetime",
            "score",
            "upvote_ratio",
            "num_comments",
            "controversiality",
            "total_awards_received",
            "stickied",
            "post_hint",
            "is_self",
            "is_video",
            "title",
            "body",
            "author",
            "author_premium",
        ]
        if self.oversized:
            self.write_path = os.getcwd() + "\\{}.csv".format(self.query)
            self.csv = open(self.write_path, "w", newline="", encoding="utf-8")
            self.csv_writer = csv.writer(self.csv, delimiter=",")
            self.csv_writer.writerow(self.headers)
        self.df = pd.DataFrame(columns=self.headers)
        self.submissions = self.df.copy()
        self.comments = self.df.copy()

        def search(self, _post_type):
            """
            Helper function to parse comment json objects.
            """

            for line, file_bytes_processed in self.read_lines_zst():
                self.line_counter += 1
                if self.line_counter % 1000000 == 0:
                    print(
                        "  >> Processed {} Posts, Found {} Posts".format(
                            self.line_counter, self.post_counter
                        )
                    )
                try:
                    _post = json.loads(line)
                    if self.type == "subreddit":
                        if int(_post["created_utc"]) >= int(self.after):
                            if int(_post["created_utc"]) <= int(self.before):
                                if _post["subreddit"] == self.query:
                                    self.post_counter += 1
                                    post_data = self.create_common_data(
                                        p_type_=_post_type, post=_post
                                    )
                                    if post_data == "comment":
                                        try:
                                            if self.oversized:
                                                self.csv_writer.writerow(
                                                    list(post_data.values())
                                                )
                                            else:
                                                self.comments = self.comments.append(
                                                    post_data, ignore_index=True
                                                )
                                        except KeyboardInterrupt:
                                            if self.oversized:
                                                self.csv_writer.writerow(
                                                    list(post_data.values())
                                                )
                                            else:
                                                self.comments = self.comments.append(
                                                    post_data, ignore_index=True
                                                )
                                            print(
                                                "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                                            )
                                            break
                                    elif post_data == "submission":
                                        try:
                                            if self.oversized:
                                                self.csv_writer.writerow(
                                                    list(post_data.values())
                                                )
                                            else:
                                                self.submissions = (
                                                    self.submissions.append(
                                                        post_data, ignore_index=True
                                                    )
                                                )
                                        except KeyboardInterrupt:
                                            if self.oversized:
                                                self.csv_writer.writerow(
                                                    list(post_data.values())
                                                )
                                            else:
                                                self.submissions = (
                                                    self.submissions.append(
                                                        post_data, ignore_index=True
                                                    )
                                                )
                                            print(
                                                "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                                            )
                                            break
                except (KeyError, json.JSONDecodeError):
                    self.errors += 1

        def make_time_list(self):
            """
            Helper function to create time lists to use for parsing pushshift.io downloaded files.
            """

            first = self.after_dt
            last = self.before_dt
            self.time_list = []
            while first <= last:
                self.time_list.append(first.strftime("%Y-%m"))
                first += relativedelta(months=1)
            if last.strftime("%Y-%m") in self.time_list:
                pass
            else:
                self.time_list.append(last.strftime("%Y-%m"))

        make_time_list(self=self)

        all_submission_files = [
            submission_file for submission_file in self.submission_folder_path.iterdir()
        ]
        if self.post_type == "comment":
            pass
        else:
            for file in all_submission_files:
                try:
                    for time in self.time_list:
                        if time in file.name:
                            self.working_file = str(file.as_posix())
                            print("> Parsing : {}".format(file.name))
                            try:
                                search(self=self, _post_type="submission")
                            except KeyboardInterrupt:
                                print(
                                    "Keyboard Interrupt Detected, your object's values are secure"
                                )
                                break
                            self.file_counter += 1
                            print(
                                "   >>> Total Files Parsed : {}, Total Posts Parsed : {}, Total Posts Collected : {}, Total Errors Found : {}".format(
                                    self.file_counter,
                                    self.line_counter,
                                    self.post_counter,
                                    self.errors,
                                )
                            )
                except KeyboardInterrupt:
                    print(
                        "Keyboard Interrupt Detected, your object's values are secure"
                    )
                    break

        all_comment_files = [
            comment_file for comment_file in self.comment_folder_path.iterdir()
        ]
        if self.post_type == "submission":
            pass
        else:
            for file in all_comment_files:
                try:
                    for time in self.time_list:
                        if time in file.name:
                            self.working_file = str(file.as_posix())
                            print("> Parsing : {}".format(file.name))
                            try:
                                search(self=self, _post_type="comment")
                            except KeyboardInterrupt:
                                print(
                                    "Keyboard Interrupt Detected, your object's values are secure"
                                )
                                break
                            self.file_counter += 1
                            print(
                                "   >>> Total Files Parsed : {}, Total Posts Parsed : {}, Total Posts Collected : {}, Total Errors Found : {}".format(
                                    self.file_counter,
                                    self.line_counter,
                                    self.post_counter,
                                    self.errors,
                                )
                            )
                except KeyboardInterrupt:
                    print(
                        "Keyboard Interrupt Detected, your object's values are secure"
                    )
                    break

        if self.oversized:
            self.df = pd.read_csv(self.write_path, low_memory=False)
        else:
            self.df = self.submissions.append(self.comments)

    def export(self, path, to_export="df", export_format="pkl"):
        """
        Easily save and export your data for future analytics.
        ----------
        paramaters
        ----------
        path: path to save output data to.
        to_export: select what data you wish to export
            'df'- all data.
            'submissions'- only submission data.
            'comments'- only comment data.
        export_format:
            '.pkl'- default, exports to pickle.
            '.csv'- export to comma seperated file.
        """

        if to_export == "df":
            if export_format == "pkl":
                self.df.to_pickle(path=path)
            elif export_format == "csv":
                self.df.to_csv(path_or_buf=path)
        elif to_export == "submissions":
            if export_format == "pkl":
                self.submissions.to_pickle(path=path)
            elif export_format == "csv":
                self.submissions.to_csv(path_or_buf=path)
        elif to_export == "comments":
            if export_format == "pkl":
                self.comments.to_pickle(path=path)
            elif export_format == "csv":
                self.comments.to_csv(path_or_buf=path)


# Create pushshift web query object
@dataclass
class pushshift_web_query(query):
    """
    Class for compiling pushshift web queries.
    ----------
    paramaters
    ----------
    query_type:
        subreddit- query provided subreddit.
        keyword- query all subreddits for provided keyword.
    query: provided subreddit or keyword.
    time_range: dictionary input {'before' : latest post time, 'after' : earliest post time}
        times can be given in unix epoch timestamp or datetime format.
    time_format:
        'unix'- defaults to unix epoch timestamp.
        'datetime'- set this option is specifing time_range in datetime format.
    post_type: selection to query for comments or submissions, defaults to both.
        'comment'- only query comments.
        'submission'- only query submission.
        defaults to query both comments and submissions.
    """

    def __init__(
        self, query_type, query, time_range, time_format="unix", post_type=None
    ):
        """
        Initilization of query object.
        """
        super().__init__(query_type, query, time_range, time_format, post_type)
        self.api_hit_counter = 0

    def update_url(self):
        """
        Helper function to update timestamp after each API request.
        """

        try:
            if self.type == "subreddit":
                self.comment_url = "https://api.pushshift.io/reddit/search/{}/?after={}&before={}&subreddit={}&size={}".format(
                    str("comment"),
                    str(self.current_time),
                    str(self.before),
                    str(self.query),
                    "12345",
                )
                self.submission_url = "https://api.pushshift.io/reddit/search/{}/?after={}&before={}&subreddit={}&size={}".format(
                    str("submission"),
                    str(self.current_time),
                    str(self.before),
                    str(self.query),
                    "12345",
                )
            elif self.type == "keyword":
                self.comment_url = "https://api.pushshift.io/reddit/search/{}/?q={}&after={}&before={}&size={}".format(
                    str("comment"),
                    str(self.query),
                    str(self.current_time),
                    str(self.before),
                    "12345",
                )
                self.submission_url = "https://api.pushshift.io/reddit/search/{}/?q={}&after={}&before={}&size={}".format(
                    str("submission"),
                    str(self.query),
                    str(self.current_time),
                    str(self.before),
                    "12345",
                )
        except KeyboardInterrupt:
            pass

    def make_query(self, oversized=False, _path=None):
        """
        Initialize the query.
        """

        self.oversized = oversized
        self.headers = [
            "post_type",
            "subreddit",
            "id",
            "parent_id",
            "link_id",
            "url",
            "permalink",
            "created_utc",
            "datetime",
            "score",
            "upvote_ratio",
            "num_comments",
            "controversiality",
            "total_awards_received",
            "stickied",
            "post_hint",
            "is_self",
            "is_video",
            "title",
            "body",
            "author",
            "author_premium",
        ]
        if self.oversized:
            if _path == None:
                self.write_path = os.getcwd() + "\\{}.csv".format(self.query)
            else:
                self.write_path = _path
            self.csv = open(self.write_path, "w", newline="", encoding="utf-8")
            self.csv_writer = csv.writer(self.csv, delimiter=",")
            self.csv_writer.writerow(self.headers)
        self.df = pd.DataFrame(columns=self.headers)
        self.submissions = self.df.copy()
        self.comments = self.df.copy()

        def web_hit(self, url):
            """
            Helper function to make the API request.
            ----------
            paramaters
            ----------
            url: provide either self.submission_url or self.comment_url depending on post type.
            """

            try:
                self.api_hit_counter += 1
                try:
                    r = requests.get(url)
                    status = r.status_code
                    print("> http response is:", status)
                except:
                    status = "NO HANDSHAKE WITH API"
                    print(status)
                if status != 200:
                    retry = 0
                    while True:
                        retry += 1
                        print(
                            "\nAPI DECLINED REQUEST\n\n>> This is retry #:",
                            retry,
                            "<<\n",
                        )
                        time.sleep(5 * retry)
                        try:
                            r = requests.get(url)
                            status = r.status_code
                            print(">> retry http response is:", status)
                        except:
                            status = "NO HANDSHAKE WITH API"
                            print(status)
                        if status == 200:
                            break
                print(" >> Web Hit On", self.query, "# :", self.api_hit_counter)
                print(
                    "  >>> Current Post Time :",
                    str(datetime.fromtimestamp(self.current_time)),
                )
                self.web_data = json.loads(r.text, strict=False)
                time.sleep(0.5)
            except KeyboardInterrupt:
                pass

        def save(self, _post_type):
            """
            Helper function to save comments to self.comments.
            """

            for _post in self.web_data["data"]:
                post_data = self.create_common_data(p_type_=_post_type, post=_post)
                if _post_type == "comment":
                    try:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.comments = self.comments.append(
                                post_data, ignore_index=True
                            )
                        self.current_time = post_data["created_utc"]
                    except KeyboardInterrupt:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.comments = self.comments.append(
                                post_data, ignore_index=True
                            )
                        self.current_time = post_data["created_utc"]
                        print(
                            "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                        )
                        break
                elif _post_type == "submission":
                    try:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.submissions = self.submissions.append(
                                post_data, ignore_index=True
                            )
                        self.current_time = post_data["created_utc"]
                    except KeyboardInterrupt:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.submissions = self.submissions.append(
                                post_data, ignore_index=True
                            )
                        self.current_time = post_data["created_utc"]
                        print(
                            "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                        )
                        break

        def collect_submissions(self):
            """
            Master function to chain previous helper functions and collect the requested data for submissions.
            """

            self.current_time = self.after
            if self.post_type == "comment":
                pass
            else:
                while self.current_time < self.before:
                    self.update_url()
                    web_hit(self=self, url=self.submission_url)
                    if len(self.web_data["data"]) == 0:
                        break
                    else:
                        try:
                            save(self=self, _post_type="submission")
                        except KeyboardInterrupt:
                            print(
                                "Keyboard Interrupt Detected, your object's values are secure"
                            )
                            break

        def collect_comments(self):
            """
            Master function to chain previous helper functions and collect the requested data for comments.
            """

            self.current_time = self.after
            if self.post_type == "submission":
                pass
            else:
                while self.current_time < self.before:
                    self.update_url()
                    web_hit(self=self, url=self.comment_url)
                    if len(self.web_data["data"]) == 0:
                        break
                    else:
                        try:
                            save(self=self, _post_type="comment")
                        except KeyboardInterrupt:
                            print(
                                "Keyboard Interrupt Detected, your object's values are secure"
                            )
                            break

        collect_submissions(self=self)
        collect_comments(self=self)
        if self.oversized:
            self.df = pd.read_csv(self.write_path, low_memory=False)
        else:
            self.df = self.submissions.append(self.comments)

    def export(self, path, to_export="df", export_format="pkl"):
        """
        Easily save and export your data for future analytics.
        ----------
        paramaters
        ----------
        path: path to save output data to.
        to_export: select what data you wish to export
            'df'- all data.
            'submissions'- only submission data.
            'comments'- only comment data.
        export_format:
            '.pkl'- default, exports to pickle.
            '.csv'- export to comma seperated file.
        """

        if to_export == "df":
            if export_format == "pkl":
                self.df.to_pickle(path=path)
            elif export_format == "csv":
                self.df.to_csv(path_or_buf=path)
        elif to_export == "submissions":
            if export_format == "pkl":
                self.submissions.to_pickle(path=path)
            elif export_format == "csv":
                self.submissions.to_csv(path_or_buf=path)
        elif to_export == "comments":
            if export_format == "pkl":
                self.comments.to_pickle(path=path)
            elif export_format == "csv":
                self.comments.to_csv(path_or_buf=path)


# Create community object
@dataclass
class community:
    def __init__(
        self,
        name="community",
        path=None,
        dataframe=None,
        columns=None,
        file_format=None,
    ):
        """
        Initilization of object, created DataFrame for provided community.
        ----------
        paramaters
        ----------
        path: path to file location.
        datafram: pass a corresponding community dataframe.
        columns: selected colums to read, only applicable for .csv.
        file_format: defaults to "None" when passed a pandas dataframe.
            "csv"- for passing DataFrame stored as csv.
            "pkl"- for passing a pickled DataFrame.
        """

        self.name = name
        if path == None:
            self.df = dataframe
        elif file_format == "csv":
            self.df = pd.read_csv(
                filepath_or_buffer=path,
                usecols=columns,
                index_col=False,
                low_memory=False,
            )
        elif file_format == "pkl":
            self.df = pd.read_pickle(filepath_or_buffer=path)
        self.df.set_index("id", inplace=True)
        submission_mask = self.df["post_type"] == "submission"
        comment_mask = self.df["post_type"] == "comment"
        self.submissions = self.df[submission_mask]
        self.comments = self.df[comment_mask]

    def urls(self, column="body", post_type=None):
        """
        Qurey posts for url embeddings in posts.
        ----------
        paramaters
        ----------
        column: DataFrame column to query on, body or title.
        post_type: option to restrict posts to only comments or submissions.
        Note* submissions usuallly contain a seperate embedded url field.
        """

        url_pattern = re.compile(r"((www\.[^\s]+)|(https://[^\s]+))")
        url_pattern1 = re.compile(r"^www\.[^\s]+")
        url_pattern2 = re.compile(r"https://[^\s]+")
        subreddit_pattern = re.compile(r"(.*reddit.com/r/)([\w]+)(/.*)")

        def find_urls(frame):
            """
            Helper function to parse regex findall on urls.
            """

            mask = frame[column].str.match(url_pattern, na=False)
            self.text_url_df = frame[mask]
            subreddits = []
            for matches in self.text_url_df[column].str.findall(url_pattern1):
                if matches == []:
                    pass
                for match in matches:
                    subreddits.append(match[:])
            for matches in self.text_url_df[column].str.findall(url_pattern2):
                if matches == []:
                    pass
                for match in matches:
                    subreddits.append(match[:])
            self.text_urls = pd.DataFrame(pd.Series(subreddits).value_counts())
            self.text_urls.columns = ["count"]
            self.text_urls.index = self.text_urls.index.str.replace(
                r"\\_", r"_", regex=True
            )
            self.text_url_references = pd.DataFrame(
                self.text_urls.index.str.extract(subreddit_pattern)[1].value_counts()
            )
            self.text_url_references.columns = ["count"]
            self.text_url_references.index.name = "url"

        if post_type == None:
            find_urls(frame=self.df)
        elif post_type == "comment":
            comment_mask = self.df["post_type"] == "comment"
            find_urls(frame=self.df[comment_mask])
        elif post_type == "submission":
            submission_mask = self.df["post_type"] == "submission"
            find_urls(frame=self.df[submission_mask])

    def references(self, column="body", post_type=None):
        """
        Qurey posts for r/subreddit reference embeddings in posts.
        ----------
        paramaters
        ----------
        column: DataFrame column to query on, body or title.
        post_type: option to restrict posts to only comments or submissions.
        Note* submissions usuallly contain a seperate embedded url field.
        """

        find_r_slash_refs = re.compile(r"r/[\w]+")
        stop_words = ["it", "they", "and", "the"]
        subreddits = []

        def find_references(frame):
            for matches in frame[column].dropna().str.findall(find_r_slash_refs):
                if matches == []:
                    pass
                for match in matches:
                    if match[2:] not in stop_words:
                        subreddits.append(match[2:])
            self.r_slash_references = pd.DataFrame(pd.Series(subreddits).value_counts())
            self.r_slash_references.columns = ["count"]
            self.r_slash_references.index.name = "r/subreddit"

        if post_type == None:
            find_references(frame=self.df)
        elif post_type == "comment":
            comment_mask = self.df["post_type"] == "comment"
            find_references(frame=self.df[comment_mask])
        elif post_type == "submission":
            submission_mask = self.df["post_type"] == "submission"
            find_references(frame=self.df[submission_mask])

        return self.r_slash_references

    def authors(self):
        """
        Create author referencing DataFrame for the community of interest.
        """

        indx = self.df["author"].unique()
        self.authors = pd.DataFrame(
            columns=[
                "total_submissions",
                "total_submission_score",
                "total_submission_comments",
                "total_comments",
                "total_comment_score",
                "total_posts",
                "total_post_score",
            ],
            index=indx,
        )
        type_mask = self.df["post_type"] == "submission"
        total_submissions = self.df[type_mask].groupby("author").size()
        self.authors["total_submissions"] = total_submissions
        total_submission_score = self.df[type_mask].groupby("author")["score"].sum()
        self.authors["total_submission_score"] = total_submission_score
        total_submission_comments = (
            self.df[type_mask].groupby("author")["num_comments"].sum()
        )
        self.authors["total_submission_comments"] = total_submission_comments
        total_comments = self.df[~type_mask].groupby("author").size()
        self.authors["total_comments"] = total_comments
        total_comment_score = self.df[~type_mask].groupby("author")["score"].sum()
        self.authors["total_comment_score"] = total_comment_score
        self.authors = self.authors.apply(lambda x: x.fillna(0), axis=1)
        self.authors["total_posts"] = (
            self.authors["total_submissions"] + self.authors["total_comments"]
        )
        self.authors["total_post_score"] = (
            self.authors["total_submission_score"] + self.authors["total_comment_score"]
        )
        self.authors.index.name = "author"

        return self.authors

    def compare_authors(self, community):
        """
        Perform outer and inner joins on the two community authors.
        returns a tuple containing: (outer join DataFrame, inner join DataFrame)
        """

        outer = pd.merge(
            self.authors[["total_submissions", "total_comments", "total_posts"]],
            community.authors[["total_submissions", "total_comments", "total_posts"]],
            how="outer",
            left_index=True,
            right_index=True,
            suffixes=("_" + self.name, "_" + community.name),
        ).fillna(0)
        inner = pd.merge(
            self.authors[["total_submissions", "total_comments", "total_posts"]],
            community.authors[["total_submissions", "total_comments", "total_posts"]],
            how="inner",
            left_index=True,
            right_index=True,
            suffixes=("_" + self.name, "_" + community.name),
        ).fillna(0)

        return outer, inner

    def gini(self):
        """
        Calculate Gini Coefficient for Community Authors.
        """

        def make_gini(df, column, min_val=1, drop=False):
            gini_base = df[column].loc[~(df[column] < (min_val))]
            if drop != False:
                gini_base.drop(drop, inplace=True)
            users = gini_base.index
            n = len(users)
            numer = 0
            for u in users:
                numer += np.sum(np.abs(gini_base - gini_base[u]))
            gini = numer / (2 * n * gini_base.sum())

            return gini

        gini_cols = [
            "total_submissions",
            "total_submission_score",
            "total_comments",
            "total_comment_score",
        ]
        try:
            gini_dict = {
                col + "_gini": make_gini(self.authors, col, drop="[deleted]")
                for col in gini_cols
            }
        except:
            self.authors()
            gini_dict = {
                col + "_gini": make_gini(self.authors, col, drop="[deleted]")
                for col in gini_cols
            }
        self.gini = pd.DataFrame(data=gini_dict, index=[self.name])

        return self.gini

    def simpson(self):
        """
        Calculate Simpson-Gini Index for Community Authors.
        """

        def make_simpson(df, column, min_val=1, drop=False):
            simpson_base = df[column].loc[~(df[column] < (min_val))]
            if drop != False:
                simpson_base.drop(drop, inplace=True)
            simpson = 1 - ((simpson_base) * (simpson_base - 1)).sum() / (
                (simpson_base.sum()) * (simpson_base.sum() - 1)
            )

            return simpson

        simpson_cols = [
            "total_submissions",
            "total_submission_score",
            "total_comments",
            "total_comment_score",
        ]
        try:
            simpson_dict = {
                col + "_simpson": make_simpson(self.authors, col, drop="[deleted]")
                for col in simpson_cols
            }
        except:
            self.authors()
            simpson_dict = {
                col + "_simpson": make_simpson(self.authors, col, drop="[deleted]")
                for col in simpson_cols
            }
        self.simpson = pd.DataFrame(data=simpson_dict, index=[self.name])

        return self.simpson

    def shannon(self):
        """
        Calculate Shannon Entropy for Community Authors.
        """

        def make_shannon(df, column, min_val=1, drop=False):
            shannon_base = df[column].loc[(df[column] >= (min_val))]
            if drop != False:
                shannon_base.drop(drop, inplace=True)
            p = shannon_base / shannon_base.sum()
            p = -p * np.log2(p)
            shannon = p.sum()

            return shannon

        shannon_cols = [
            "total_submissions",
            "total_submission_score",
            "total_comments",
            "total_comment_score",
        ]
        try:
            shannon_dict = {
                col + "_shannon": make_shannon(self.authors, col, drop="[deleted]")
                for col in shannon_cols
            }
        except:
            self.authors()
            shannon_dict = {
                col + "_shannon": make_shannon(self.authors, col, drop="[deleted]")
                for col in shannon_cols
            }
        self.shannon = pd.DataFrame(data=shannon_dict, index=[self.name])

        return self.shannon

    def dates(self):
        """
        Convert unix epochs to datetime.
        """

        self.dates = pd.to_datetime(self.df["datetime"]).dt.dayofyear / 365

        return self.dates

    def labels(self):
        """
        Create feature labels.
        """

        subreddit_pattern = re.compile(r"(.*reddit.com/r/)([\w]+)(/.*)")
        find_r_slash_refs = re.compile(r"(.*\br/)([\w]+)(\b.*)")
        self.url_ref = self.df["url"].str.extract(subreddit_pattern)[1]
        self.url_ref = self.url_ref.str.lower()
        self.body_ref = self.df["body"].str.extract(find_r_slash_refs)[1]
        self.body_ref = self.body_ref.str.lower()
        self.title_ref = self.df["title"].str.extract(find_r_slash_refs)[1]
        self.title_ref = self.title_ref.str.lower()

    def features(
        self,
        drop=[
            "author_community_total_posts",
            "author_community_total_post_score",
            "url_ref",
            "body_ref",
            "url_label",
            "body_label",
        ],
    ):
        """
        Create feature dataframe.
        ----------
        paramaters
        ----------
        drop: DataFrame column to remove from feature anlysis.
        """

        self.features = pd.DataFrame()
        self.features["post_type"] = self.df["post_type"].copy()
        self.features["post_type"].replace("submission", 1, inplace=True)
        self.features["post_type"].replace("comment", 0, inplace=True)
        self.features["year"] = pd.to_datetime(self.df["datetime"]).dt.year
        self.features["day_of_year"] = (
            pd.to_datetime(self.df["datetime"]).dt.dayofyear / 365
        )
        seconds_in_day = 60 * 60 * 24
        self.features["time_of_day"] = self.df["created_utc"].apply(
            lambda x: x // seconds_in_day / seconds_in_day
        )

        for column in self.authors().columns:
            author_attribute_dictionary = self.authors[column].to_dict()
            feature_name = "author_community_" + column
            self.features[feature_name] = self.df["author"].apply(
                lambda x: author_attribute_dictionary[x]
            )

        G = self.gini()
        self.features["total_submissions_gini"] = G.iloc[0, 0]
        self.features["total_submission_score_gini"] = G.iloc[0, 1]
        self.features["total_comments_gini"] = G.iloc[0, 2]
        self.features["total_comment_score_gini"] = G.iloc[0, 3]

        sG = self.simpson()
        self.features["total_submissions_simpson"] = sG.iloc[0, 0]
        self.features["total_submission_score_simpson"] = sG.iloc[0, 1]
        self.features["total_comments_simpson"] = sG.iloc[0, 2]
        self.features["total_comment_score_simpson"] = sG.iloc[0, 3]

        S = self.shannon()
        self.features["total_submissions_shannon"] = S.iloc[0, 0]
        self.features["total_submission_score_shannon"] = S.iloc[0, 1]
        self.features["total_comments_shannon"] = S.iloc[0, 2]
        self.features["total_comment_score_shannon"] = S.iloc[0, 3]

        self.labels()
        self.features["url_ref"] = self.url_ref
        self.features["body_ref"] = self.body_ref

        boolean_features = [
            "stickied",
            "is_self",
            "is_video",
            "author_premium",
        ]
        for feature in boolean_features:
            self.features[feature] = boolean_scaler(feature, self.df)

        numeric_features = [
            "score",
            "num_comments",
            "controversiality",
            "total_awards_received",
            "author_community_total_submissions",
            "author_community_total_submission_score",
            "author_community_total_submission_comments",
            "author_community_total_comments",
            "author_community_total_comment_score",
            "year",
            "total_submissions_shannon",
            "total_submission_score_shannon",
            "total_comments_shannon",
            "total_comment_score_shannon",
        ]
        for feature in numeric_features:
            try:
                self.features[feature] = numeric_scaler(feature, self.df)
            except:
                self.features[feature] = numeric_scaler(feature, self.features)

        string_features = [
            "author",
            "subreddit",
            "link_id",
            "parent_id",
            "post_hint",
        ]
        for feature in string_features:
            self.features[feature] = string_scaler(feature, self.df)

        def ref_label(ref):
            if type(ref) == str:
                ref = ref.lower()
            if ref in [
                "actualconspiracies",
                "conspiracy",
                "conspiracytheories",
                "conspiracyultra",
                "conspiracy_commons",
                "flatearthscience",
                "flatearth",
                "globeskeptic",
                "israeldid911",
                "pedogate",
                "qtheory",
                "topconspiracy",
            ]:
                return 1
            else:
                return 0

        self.features["url_label"] = self.features["url_ref"].apply(
            lambda x: ref_label(x)
        )
        self.features["body_label"] = self.features["body_ref"].apply(
            lambda x: ref_label(x)
        )
        self.features["label"] = (
            self.features["url_label"] + self.features["body_label"]
        )

        def label(val):
            if val > 0:
                return 1
            else:
                return 0

        self.features["label"] = self.features["label"].apply(lambda x: label(x))

        if drop != False:
            for feature in drop:
                self.features.drop(feature, axis=1, inplace=True)

        self.features.fillna(0, inplace=True)

        submission_mask = self.features["post_type"] == 1
        comment_mask = self.features["post_type"] == 0
        self.feature_comments = self.features[submission_mask]
        self.feature_submissions = self.features[comment_mask]

        return self.features


# Create modeling reference object
@dataclass
class modeling:
    """
    Class for predictive modeling.
    """

    def __init__(self, df, rs=0, timeseries=True):
        """
        Initilization of object, splits into train-test.
        ----------
        paramaters
        ----------
        df: dataframe to model
        rs: random state for reproducability
        timeseries: default True, split data sequentially. If False split by random sample.
        """

        self.rs = rs
        self.y = df.label
        self.X = df.drop("label", axis=1)
        if timeseries:
            l = len(self.y)
            self.y_train = self.y[: (3 * l / 4)]
            self.y_test = self.y[(3 * l / 4) :]
            self.X_train = self.X[: (3 * l / 4)]
            self.X_test = self.X[(3 * l / 4) :]
        else:
            self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
                self.X, self.y, random_state=rs
            )
        self.confusion = {}
        self.models = {}
        self.predictions = {}
        self.scores = {}
        self.precision_recall_thresh = {}
        self.fpr_tpr_thresh = {}

    def resample_train(self, method="up"):
        data = pd.concat([self.X_train, self.y_train], axis=1)
        F = data[data.label == 0]
        T = data[data.label == 1]

        if method == "up":
            upsampled = resample(
                T, replace=True, n_samples=len(F), random_state=self.rs
            )
            resampled = pd.concat([F, upsampled])

        if method == "down":
            downsampled = resample(
                F, replace=True, n_samples=len(T), random_state=self.rs
            )
            resampled = pd.concat([downsampled, T])

        self.y_train = resampled.label
        self.X_train = resampled.drop("label", axis=1)

    def score(self, model):
        _accuracy = accuracy_score(self.y_test, self.predictions[model])
        _precison = precision_score(self.y_test, self.predictions[model])
        _recall = recall_score(self.y_test, self.predictions[model])
        _f1 = f1_score(self.y_test, self.predictions[model])
        _auc = auc(self.fpr_tpr_thresh[model][0], self.fpr_tpr_thresh[model][1])
        self.scores[model] = {
            "accuracy": _accuracy,
            "precison": _precison,
            "recall": _recall,
            "f1": _f1,
            "auc": _auc,
        }

        return self.scores[model]

    def prt(self, model):
        self.precision_recall_thresh[model] = precision_recall_curve(
            self.y_test, self.predictions[model]
        )

        return self.precision_recall_thresh[model]

    def fptpt(self, model):
        self.fpr_tpr_thresh[model] = roc_curve(self.y_test, self.predictions[model])

        return self.fpr_tpr_thresh[model]

    def plot_pr_curve(self, model):
        plt.figure()
        plt.plot(
            self.precision_recall_thresh[model][0],
            self.precision_recall_thresh[model][1],
            linewidth=3,
        )
        plt.title("PRC")
        plt.xlabel("Precision")
        plt.ylabel("Recall")

        return plt.show()

    def plot_ro_curve(self, model):
        plt.figure()
        plt.plot(
            self.fpr_tpr_thresh[model][0], self.fpr_tpr_thresh[model][1], linewidth=3
        )
        plt.title("ROC")
        plt.xlabel("FP \%")
        plt.ylabel("TP \%")

        return plt.show()

    def linear_regression(self, mi=1000000, rfe=False):
        self.lr = LogisticRegression(max_iter=mi, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        if rfe:
            self.rfe = RFE(self.lr).fit(self.X_train, self.y_train)
            self.supports = {
                self.X.columns[col]: self.rfe.ranking_[col]
                for col in range(len(self.X.columns))
            }
        self.models["lr"] = self.lr
        self.predictions["lr"] = self.lr.predict(self.X_test)
        self.confusion["lr"] = confusion_matrix(self.y_test, self.predictions["lr"])
        self.prt("lr")
        self.fptpt("lr")

        return self.lr

    def decision_tree(self, md=100):
        self.dt = DecisionTreeClassifier(max_depth=md, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        self.models["dt"] = self.dt
        self.predictions["dt"] = self.dt.predict(self.X_test)
        self.confusion["dt"] = confusion_matrix(self.y_test, self.predictions["dt"])
        self.prt("dt")
        self.fptpt("dt")

        return self.dt

    def random_forest(self, ne=10):
        self.rf = RandomForestClassifier(n_estimators=ne, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        self.models["rf"] = self.rf
        self.predictions["rf"] = self.rf.predict(self.X_test)
        self.confusion["rf"] = confusion_matrix(self.y_test, self.predictions["rf"])
        self.prt("rf")
        self.fptpt("rf")

        return self.rf

    def support_vector_classifier(self, c=1, kern="rbf"):
        self.svc = SVC(C=c, kernel=kern, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        self.models["svc"] = self.svc
        self.predictions["svc"] = self.svc.predict(self.X_test)
        self.confusion["svc"] = confusion_matrix(self.y_test, self.predictions["svc"])
        self.prt("svc")
        self.fptpt("svc")

        return self.svc


# Create subreddit reference object
@dataclass
class subreddits:
    """
    Class for making queries based on subreddit type.
    The resulting DataFrame is meant to be used as a selection matrix based on subreddit.
    """

    def __init__(self, path=None, file_format=None):
        """
        Initilization of object, reads in subreddit list and adds datetime column.
        ----------
        paramaters
        ----------
        path: location of subredded statistics.
        file_format: formate to be specified if path is given.
            "csv"- if provided path points to .csv file.
            "pkl"- if provided path points to pickled DataFrame.
        """

        if path == None:
            self.path = "data/subreddit_list.csv"
            self.master = pd.read_csv(self.path)
        else:
            self.path = path
            if file_format == "csv":
                self.master = pd.read_csv(self.path)
            elif file_format == "pkl":
                self.master = pd.read_pickle(self.path)
        self.master["creation_datetime"] = [
            datetime.fromtimestamp(int(utc)) for utc in self.master["creation_utc"]
        ]

    def make_subreddits(self, api_credentials, path=None):
        """
        Helper function to create up to date list of all subreddits.
        ----------
        paramaters
        ----------
        api_credentials: dictionary containing your api_credentials...
            {
                "user_agent" : "user_agent",
                "user_pass" : "user_pass",
                "client_id" : "client_id",
                "client_secret" : "client_secret",
                "application_name" : "application_name"
            }
        path: output path for the data
            note: if None data will be stored as a DataFrame in self.master but not written to file.
        """

        api_handle = api_agent(api_credentials)
        if path == None:
            api_handle.make_subreddits()
        else:
            api_handle.make_subreddits(path=path)
        self.master = api_handle.subreddits_df

    def split_nsfw(self):
        """
        Create attributes containig masked DataFrames for Not Safe / Safe For Work subreddits.
        """

        nsfw_mask = self.master["nsfw_bool"] == True
        self.nsfw = self.master[nsfw_mask]
        self.sfw = self.master[~nsfw_mask]

    def split_size(self, min_subscribers=0, max_subscribers=9999999999):
        """
        Create attribute containing subreddits within a specified subscriber range.
        ----------
        paramaters
        ----------
        min_subscribers: minimum number of allowed subscribers to a subreddit.
        max_subscribers: maximum number of allowed subscribers to a subreddit.
        """

        min_size_mask = self.master["num_subscribers"] >= min_subscribers
        max_size_mask = self.master["num_subscribers"] <= max_subscribers
        self.sized = self.master[min_size_mask & max_size_mask]

    def split_creation_time_unix(
        self, min_unix_timestamp=0000000000, max_unix_timestamp=9999999999
    ):
        """
        Create attribute containing subreddits created within a specified time range.
        ----------
        paramaters
        ----------
        min_unix_timestamp: earliest allowed date of creation in unix ephoch timestamp.
        max_unix_timestamp: latest allowed date of creation in unix ephoch timestamp.
        """

        min_unix_time_mask = self.master["creation_utc"] >= min_unix_timestamp
        max_unix_time_mask = self.master["creation_utc"] <= max_unix_timestamp
        self.sized = self.master[min_unix_time_mask & max_unix_time_mask]

    def split_creation_time_date(
        self, min_datetime="2000-01-01", max_datetime="2022-02-02"
    ):
        """
        Create attribute containing subreddits created within a specified time range.
        ----------
        paramaters
        ----------
        min_datetime: earliest allowed date of creation in datetime format.
        max_datetime: latest allowed date of creation in datetime format.
        """

        min_date_time_mask = self.master["creation_datetime"] >= min_datetime
        max_date_time_mask = self.master["creation_datetime"] <= max_datetime
        self.sized = self.master[min_date_time_mask & max_date_time_mask]

    def split_multi(self, nsfw=None, sizes=None, unix_times=None, date_times=None):
        """
        Query based subreddit selction based on NSFW status, subscriber count, and creation time.
        ----------
        paramaters
        ----------
        nsfw: True or False
        sizes: dictionary input {'min_subscribers' : minimum_value, 'max_subscribers' : maximum_value}
        unix_times: dictionary input {'min_unix_timestamp' : minimum_timestamp, 'max_unix_timestamp' : maximum_timestamp}
        date_times: dictionary input {'min_datetime' : 'minimum_datetime', 'max_datetime' : 'maximum_datetime}
        """

        if nsfw == None:
            nsfw_mask = [True for _ in self.master.index]
        else:
            if nsfw == True:
                nsfw_mask = self.master["nsfw_bool"] == True
            elif nsfw == False:
                nsfw_mask = self.master["nsfw_bool"] == False
        if sizes == None:
            min_size_mask = [True for _ in self.master.index]
            max_size_mask = [True for _ in self.master.index]
        else:
            min_size_mask = self.master["num_subscribers"] >= sizes["min_subscribers"]
            max_size_mask = self.master["num_subscribers"] <= sizes["max_subscribers"]
        if unix_times == None:
            min_unix_time_mask = [True for _ in self.master.index]
            max_unix_time_mask = [True for _ in self.master.index]
        else:
            min_unix_time_mask = (
                self.master["creation_utc"] >= unix_times["min_unix_timestamp"]
            )
            max_unix_time_mask = (
                self.master["creation_utc"] <= unix_times["max_unix_timestamp"]
            )
        if date_times == None:
            min_date_time_mask = [True for _ in self.master.index]
            max_date_time_mask = [True for _ in self.master.index]
        else:
            min_date_time_mask = (
                self.master["creation_datetime"] >= date_times["min_datetime"]
            )
            max_date_time_mask = (
                self.master["creation_datetime"] <= date_times["max_datetime"]
            )

        self.multi = self.master[
            nsfw_mask
            & min_size_mask
            & max_size_mask
            & min_unix_time_mask
            & max_unix_time_mask
            & min_date_time_mask
            & max_date_time_mask
        ]


# Create class for file handling
@dataclass
class file_handler:
    """
    Class for handling data files.
    ----------
    methods
    ----------
    combine: combine multiple csv files into one.
    """

    def __init__(self):
        """
        create handler obeject.
        """

    def combine(self, headers=None, files=None, path_out=None):
        """
        function for combining multiple files.
        ----------
        paramaters
        ----------
        headers: headers to write to file, pass as list
        files: list of paths to files to combine
        path_out: path to output combined csv to.
        """

        self.headers = headers
        self.files = files
        self.path_out = path_out
        if self.path_out == None:
            self.path_out = os.getcwd()
        self.log = [
            "files = {}".format(self.files),
            "path_out = {}".format(self.path_out),
        ]
        self.errors = 0
        self.written = 0
        with open(self.path_out, "w", newline="", encoding="utf-8") as out_file:
            writer = csv.writer(out_file, delimiter=",")
            if self.headers != None:
                writer.writerow(self.headers)
            if self.files == None:
                print(
                    'file list is empty! please specify files=["file_1", ... , "file_n"]'
                )
            for file in self.files:
                with open(file, "r", newline="", encoding="utf-8") as in_file:
                    reader = csv.reader(in_file, delimiter=",")
                    next(reader, None)  # skip the headers
                    for row in reader:
                        try:
                            writer.writerow(row)
                            self.written += 1
                        except csv.Error:
                            self.log.append(row)
                            self.errors += 1
        self.log.append("rows written = {}".format(self.written))
        self.log.append("erraneous rows = {}".format(self.errors))
        print(
            "file combined, {} rows written, {} errors found. check file_handler.log for more details.".format(
                self.written, self.errors
            )
        )

        # dfs = [pd.read_csv(file, low_memory=False, names=self.headers, header=0) for file in self.files]
        # df = pd.concat([df for df in dfs], ignore_index=True)
        # df.to_csv(self.path_out)

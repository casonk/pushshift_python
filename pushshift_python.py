'''
Witten by: Cason Konzer

pushshift_python is a wrapper for reddit community analytics. 

read the docs at: https://github.com/casonk/pushshift_python/blob/master/documentation.ipynb
'''

# Import relative libraries
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
import matplotlib.pyplot as plt
from pathlib import Path
import networkx as nx
import seaborn as sns
import pandas as pd
import numpy as np
import zstandard
import requests
import datetime
import time
import math
import json
import re
import os
import csv

# Setup default plotter
plt.style.use('dark_background') 
plt.rcParams['figure.figsize'] = [16, 9]
plt.rcParams.update({'font.size': 18})
plt.rcParams.update({'text.usetex': False})

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
                datetime.datetime.timestamp(
                    datetime.datetime.strptime(time_range["before"], "%Y-%m-%d")
                )
            )
            time_range["after"] = int(
                datetime.datetime.timestamp(
                    datetime.datetime.strptime(time_range["after"], "%Y-%m-%d")
                )
            )
        self.before = int(time_range["before"])
        self.before_dt = datetime.datetime.fromtimestamp(self.before)
        self.after = int(time_range["after"])
        self.after_dt = datetime.datetime.fromtimestamp(self.after)
        try:
            self.post_type = post_type.lower()
        except:
            self.post_type = post_type

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
            "F:/Research/Funded/Ethical_Reccomendations/Python/Push_File/Submissions/RS/2019+/"
        )
        self.comment_folder_path = Path(
            "F:/Research/Funded/Ethical_Reccomendations/Python/Push_File/Comments/RC/2019+/"
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

    def make_query(self):
        """
        Initialize the query.
        """
        self.df = pd.DataFrame(
            columns=[
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
                "num_comments",
                "title",
                "body",
                "author",
            ]
        )
        self.submissions = self.df.copy()
        self.comments = self.df.copy()

        def create_common_data(post):
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
                t = datetime.datetime.fromtimestamp(created_utc)
                date = t.strftime("%m/%d/%Y")
                score = post["score"]
                try:
                    num_comments = post["num_comments"]
                except KeyError:
                    num_comments = "nan"
                try:
                    title = post["title"]
                    title = r"{}".format(title)
                except KeyError:
                    title = "nan"
                author = post["author"]
                author = r"{}".format(author)
                return (
                    subreddit,
                    post_id,
                    parent_id,
                    link_id,
                    url,
                    permalink,
                    date,
                    created_utc,
                    score,
                    num_comments,
                    title,
                    author,
                )
            except KeyboardInterrupt:
                pass

        def search_sumissions(self):
            for line, file_bytes_processed in self.read_lines_zst():
                self.line_counter += 1
                if self.line_counter % 1000000 == 0:
                    print(
                        "  >> Processed {} Posts, Found {} Posts".format(
                            self.line_counter, self.post_counter
                        )
                    )
                try:
                    post = json.loads(line)
                    if self.type == "subreddit":
                        if int(post["created_utc"]) >= int(self.after):
                            if int(post["created_utc"]) <= int(self.before):
                                if post["subreddit"] == self.query:
                                    self.post_counter += 1
                                    (
                                        subreddit,
                                        post_id,
                                        parent_id,
                                        link_id,
                                        url,
                                        permalink,
                                        date,
                                        created_utc,
                                        score,
                                        num_comments,
                                        title,
                                        author,
                                    ) = create_common_data(post=post)
                                    try:
                                        body = post["selftext"]
                                        body = r"{}".format(body)
                                    except KeyError:
                                        body = "nan"
                                    post_data = {
                                        "post_type": "submission",
                                        "subreddit": subreddit,
                                        "id": post_id,
                                        "parent_id": parent_id,
                                        "link_id": link_id,
                                        "url": url,
                                        "permalink": permalink,
                                        "created_utc": created_utc,
                                        "datetime": date,
                                        "score": score,
                                        "num_comments": num_comments,
                                        "title": title,
                                        "body": body,
                                        "author": author,
                                    }
                                    try:
                                        self.submissions = self.submissions.append(
                                            post_data, ignore_index=True
                                        )
                                    except KeyboardInterrupt:
                                        self.submissions = self.submissions.append(
                                            post_data, ignore_index=True
                                        )
                                        print(
                                            "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                                        )
                                        break
                            # elif self.query_type == 'keyword':
                except (KeyError, json.JSONDecodeError):
                    self.errors += 1

        def search_comments(self):
            for line, file_bytes_processed in self.read_lines_zst():
                self.line_counter += 1
                if self.line_counter % 1000000 == 0:
                    print(
                        "  >> Processed {} Posts, Found {} Posts".format(
                            self.line_counter, self.post_counter
                        )
                    )
                try:
                    post = json.loads(line)
                    if self.type == "subreddit":
                        if int(post["created_utc"]) >= int(self.after):
                            if int(post["created_utc"]) <= int(self.before):
                                if post["subreddit"] == self.query:
                                    self.post_counter += 1
                                    (
                                        subreddit,
                                        post_id,
                                        parent_id,
                                        link_id,
                                        url,
                                        permalink,
                                        date,
                                        created_utc,
                                        score,
                                        num_comments,
                                        title,
                                        author,
                                    ) = create_common_data(post=post)
                                    try:
                                        body = post["body"]
                                        body = r"{}".format(body)
                                    except KeyError:
                                        body = "nan"
                                    post_data = {
                                        "post_type": "comment",
                                        "subreddit": subreddit,
                                        "id": post_id,
                                        "parent_id": parent_id,
                                        "link_id": link_id,
                                        "url": url,
                                        "permalink": permalink,
                                        "created_utc": created_utc,
                                        "datetime": date,
                                        "score": score,
                                        "num_comments": num_comments,
                                        "title": title,
                                        "body": body,
                                        "author": author,
                                    }
                                    try:
                                        self.comments = self.comments.append(
                                            post_data, ignore_index=True
                                        )
                                    except KeyboardInterrupt:
                                        self.comments = self.comments.append(
                                            post_data, ignore_index=True
                                        )
                                        print(
                                            "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                                        )
                                        break
                            # elif self.query_type == 'keyword':
                except (KeyError, json.JSONDecodeError):
                    self.errors += 1

        def make_time_list(self):
            first = self.after_dt
            last = self.before_dt
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
                                search_sumissions(self=self)
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
                                search_comments(self=self)
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

    def make_query(self):
        """
        Initialize the query.
        """
        self.df = pd.DataFrame(
            columns=[
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
                "num_comments",
                "title",
                "body",
                "author",
            ]
        )
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
                        time.sleep(15 * retry)
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
                    str(datetime.datetime.fromtimestamp(self.current_time)),
                )
                self.web_data = json.loads(r.text, strict=False)
                time.sleep(1)
            except KeyboardInterrupt:
                pass

        def create_common_data(post):
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
                t = datetime.datetime.fromtimestamp(created_utc)
                date = t.strftime("%m/%d/%Y")
                score = post["score"]
                try:
                    num_comments = post["num_comments"]
                except KeyError:
                    num_comments = "nan"
                try:
                    title = post["title"]
                    title = r"{}".format(title)
                except KeyError:
                    title = "nan"
                author = post["author"]
                author = r"{}".format(author)
                return (
                    subreddit,
                    post_id,
                    parent_id,
                    link_id,
                    url,
                    permalink,
                    date,
                    created_utc,
                    score,
                    num_comments,
                    title,
                    author,
                )
            except KeyboardInterrupt:
                pass

        def save_submissions(self):
            """
            Helper function to save submissions to self.submissions.
            """
            for post in self.web_data["data"]:
                (
                    subreddit,
                    post_id,
                    parent_id,
                    link_id,
                    url,
                    permalink,
                    date,
                    created_utc,
                    score,
                    num_comments,
                    title,
                    author,
                ) = create_common_data(post=post)
                try:
                    body = post["selftext"]
                    body = r"{}".format(body)
                except KeyError:
                    body = "nan"
                post_data = {
                    "post_type": "submission",
                    "subreddit": subreddit,
                    "id": post_id,
                    "parent_id": parent_id,
                    "link_id": link_id,
                    "url": url,
                    "permalink": permalink,
                    "created_utc": created_utc,
                    "datetime": date,
                    "score": score,
                    "num_comments": num_comments,
                    "title": title,
                    "body": body,
                    "author": author,
                }
                try:
                    self.submissions = self.submissions.append(
                        post_data, ignore_index=True
                    )
                    self.current_time = created_utc
                except KeyboardInterrupt:
                    self.submissions = self.submissions.append(
                        post_data, ignore_index=True
                    )
                    self.current_time = created_utc
                    print(
                        "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                    )
                    break

        def save_comments(self):
            """
            Helper function to save comments to self.comments.
            """
            for post in self.web_data["data"]:
                (
                    subreddit,
                    post_id,
                    parent_id,
                    link_id,
                    url,
                    permalink,
                    date,
                    created_utc,
                    score,
                    num_comments,
                    title,
                    author,
                ) = create_common_data(post=post)
                try:
                    body = post["body"]
                    body = r"{}".format(body)
                except KeyError:
                    body = "nan"
                post_data = {
                    "post_type": "comment",
                    "subreddit": subreddit,
                    "id": post_id,
                    "parent_id": parent_id,
                    "link_id": link_id,
                    "url": url,
                    "permalink": permalink,
                    "created_utc": created_utc,
                    "datetime": date,
                    "score": score,
                    "num_comments": num_comments,
                    "title": title,
                    "body": body,
                    "author": author,
                }
                try:
                    self.comments = self.comments.append(post_data, ignore_index=True)
                    self.current_time = created_utc
                except KeyboardInterrupt:
                    self.submissions = self.submissions.append(
                        post_data, ignore_index=True
                    )
                    self.current_time = created_utc
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
                            save_submissions(self=self)
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
                            save_comments(self=self)
                        except KeyboardInterrupt:
                            print(
                                "Keyboard Interrupt Detected, your object's values are secure"
                            )
                            break

        collect_submissions(self=self)
        collect_comments(self=self)

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
            elif export_format == ".csv":
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
                filepath_or_buffer=path, usecols=columns, low_memory=False
            )
        elif file_format == "pkl":
            self.df = pd.read_pickle(filepath_or_buffer=path)
        submission_mask = self.df["post_type"] == "submission"
        comment_mask = self.df["post_type"] == "comment"
        self.submissions = self.df[submission_mask]
        self.comments = self.df[comment_mask]

    def make_urls(self, column=None, post_type=None):
        """
        Qurey posts for url embeddings in posts.
        ----------
        paramaters
        ----------
        column: DataFrame column to query on, body or title.
        post_type: option to restrict posts to only comments or submissions.
        Note* submissions usuallly contain a seperate embedded url field.
        """
        url_pattern = re.compile("((www\.[^\s]+)|(https://[^\s]+))")

        def find_urls(frame):
            mask = frame[column].str.match(url_pattern, na=False)
            self.url_df = frame[mask]
            self.urls = pd.DataFrame(
                self.url_df[column].str.extract(url_pattern)[0].rename("url")
            )

        if post_type == None:
            find_urls(frame=self.df)
        elif post_type == "comment":
            comment_mask = self.df["post_type"] == "comment"
            find_urls(frame=self.df[comment_mask])
        elif post_type == "submission":
            submission_mask = self.df["post_type"] == "submission"
            find_urls(frame=self.df[submission_mask])

    def make_authors(self):
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
            self.master = pd.read_csv(
                "F:\Research\Funded\Ethical_Reccomendations\Python\Data\Docs\subreddit_list.csv"
            )
        else:
            if file_format == "csv":
                self.master = pd.read_csv(path)
            elif file_format == "pkl":
                self.master = pd.read_pickle(path)
        self.master["Creation_DateTime"] = [
            datetime.datetime.fromtimestamp(int(utc))
            for utc in self.master["Creation_UTC"]
        ]

    def split_nsfw(self):
        """
        Create attributes containig masked DataFrames for Not Safe / Safe For Work subreddits.
        """
        nsfw_mask = self.master["NSFW_BOOL"] == True
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
        min_size_mask = self.master["#_Subscribers"] >= min_subscribers
        max_size_mask = self.master["#_Subscribers"] <= max_subscribers
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
        min_unix_time_mask = self.master["Creation_UTC"] >= min_unix_timestamp
        max_unix_time_mask = self.master["Creation_UTC"] <= max_unix_timestamp
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
        min_date_time_mask = self.master["Creation_DateTime"] >= min_datetime
        max_date_time_mask = self.master["Creation_DateTime"] <= max_datetime
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
                nsfw_mask = self.master["NSFW_BOOL"] == True
            elif nsfw == False:
                nsfw_mask = self.master["NSFW_BOOL"] == False
        if sizes == None:
            min_size_mask = [True for _ in self.master.index]
            max_size_mask = [True for _ in self.master.index]
        else:
            min_size_mask = self.master["#_Subscribers"] >= sizes["min_subscribers"]
            max_size_mask = self.master["#_Subscribers"] <= sizes["max_subscribers"]
        if unix_times == None:
            min_unix_time_mask = [True for _ in self.master.index]
            max_unix_time_mask = [True for _ in self.master.index]
        else:
            min_unix_time_mask = (
                self.master["Creation_UTC"] >= unix_times["min_unix_timestamp"]
            )
            max_unix_time_mask = (
                self.master["Creation_UTC"] <= unix_times["max_unix_timestamp"]
            )
        if date_times == None:
            min_date_time_mask = [True for _ in self.master.index]
            max_date_time_mask = [True for _ in self.master.index]
        else:
            min_date_time_mask = (
                self.master["Creation_DateTime"] >= date_times["min_datetime"]
            )
            max_date_time_mask = (
                self.master["Creation_DateTime"] <= date_times["max_datetime"]
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

import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import zstandard
from dateutil.relativedelta import relativedelta

from _pushshift_scalers import append_row


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

    def __init__(self, query_type, query, time_range, time_format="unix", post_type=None):
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
                title = rf"{title}"
            except KeyError:
                title = "nan"
            author = post["author"]
            author = rf"{author}"
            try:
                author_premium = post["author_premium"]
            except:
                author_premium = "nan"
            if p_type_ == "comment":
                try:
                    body = post["body"]
                    body = rf"{body}"
                except KeyError:
                    body = "nan"
            elif p_type_ == "submission":
                try:
                    body = post["selftext"]
                    body = rf"{body}"
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

    def __init__(self, query_type, query, time_range, time_format="unix", post_type=None):
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
            reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
            while True:
                chunk = reader.read(2**27).decode()
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")

                for line in lines[:-1]:
                    yield line, file_handle.tell()

                buffer = lines[-1]
            if buffer:
                yield buffer, file_handle.tell()
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
            self.write_path = str(Path.cwd() / f"{self.query}.csv")
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

            for line, _file_bytes_processed in self.read_lines_zst():
                self.line_counter += 1
                if self.line_counter % 1000000 == 0:
                    print(
                        f"  >> Processed {self.line_counter} Posts, Found {self.post_counter} Posts"
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
                                    if post_data["post_type"] == "comment":
                                        try:
                                            if self.oversized:
                                                self.csv_writer.writerow(list(post_data.values()))
                                            else:
                                                self.comments = append_row(self.comments, post_data)
                                        except KeyboardInterrupt:
                                            if self.oversized:
                                                self.csv_writer.writerow(list(post_data.values()))
                                            else:
                                                self.comments = append_row(self.comments, post_data)
                                            print(
                                                "Keyboard Interrupt Detected, please Interrupt again to break parent function."
                                            )
                                            break
                                    elif post_data["post_type"] == "submission":
                                        try:
                                            if self.oversized:
                                                self.csv_writer.writerow(list(post_data.values()))
                                            else:
                                                self.submissions = append_row(
                                                    self.submissions, post_data
                                                )
                                        except KeyboardInterrupt:
                                            if self.oversized:
                                                self.csv_writer.writerow(list(post_data.values()))
                                            else:
                                                self.submissions = append_row(
                                                    self.submissions, post_data
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
                            print(f"> Parsing : {file.name}")
                            try:
                                search(self=self, _post_type="submission")
                            except KeyboardInterrupt:
                                print(
                                    "Keyboard Interrupt Detected, your object's values are secure"
                                )
                                break
                            self.file_counter += 1
                            print(
                                f"   >>> Total Files Parsed : {self.file_counter}, Total Posts Parsed : {self.line_counter}, Total Posts Collected : {self.post_counter}, Total Errors Found : {self.errors}"
                            )
                except KeyboardInterrupt:
                    print("Keyboard Interrupt Detected, your object's values are secure")
                    break

        all_comment_files = [comment_file for comment_file in self.comment_folder_path.iterdir()]
        if self.post_type == "submission":
            pass
        else:
            for file in all_comment_files:
                try:
                    for time in self.time_list:
                        if time in file.name:
                            self.working_file = str(file.as_posix())
                            print(f"> Parsing : {file.name}")
                            try:
                                search(self=self, _post_type="comment")
                            except KeyboardInterrupt:
                                print(
                                    "Keyboard Interrupt Detected, your object's values are secure"
                                )
                                break
                            self.file_counter += 1
                            print(
                                f"   >>> Total Files Parsed : {self.file_counter}, Total Posts Parsed : {self.line_counter}, Total Posts Collected : {self.post_counter}, Total Errors Found : {self.errors}"
                            )
                except KeyboardInterrupt:
                    print("Keyboard Interrupt Detected, your object's values are secure")
                    break

        if self.oversized:
            self.csv.close()
            self.df = pd.read_csv(self.write_path, low_memory=False)
        else:
            self.df = pd.concat([self.submissions, self.comments], ignore_index=True)

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

    def __init__(self, query_type, query, time_range, time_format="unix", post_type=None):
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
                    "comment",
                    str(self.current_time),
                    str(self.before),
                    str(self.query),
                    "12345",
                )
                self.submission_url = "https://api.pushshift.io/reddit/search/{}/?after={}&before={}&subreddit={}&size={}".format(
                    "submission",
                    str(self.current_time),
                    str(self.before),
                    str(self.query),
                    "12345",
                )
            elif self.type == "keyword":
                self.comment_url = "https://api.pushshift.io/reddit/search/{}/?q={}&after={}&before={}&size={}".format(
                    "comment",
                    str(self.query),
                    str(self.current_time),
                    str(self.before),
                    "12345",
                )
                self.submission_url = "https://api.pushshift.io/reddit/search/{}/?q={}&after={}&before={}&size={}".format(
                    "submission",
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
            if _path is None:
                self.write_path = str(Path.cwd() / f"{self.query}.csv")
            else:
                self.write_path = _path
            self.csv = open(self.write_path, "w", newline="", encoding="utf-8")
            self.csv_writer = csv.writer(self.csv, delimiter=",", escapechar="\\")
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
                        time.sleep(2.5 * retry)
                        try:
                            r = requests.get(url)
                            status = r.status_code
                            print("> retry http response is:", status)
                        except:
                            status = "NO HANDSHAKE WITH API"
                            print(status)
                        if status == 200:
                            break
                print("  >> Web Hit On", self.query, "# :", self.api_hit_counter)
                print(
                    "    >>> Current Post Time :",
                    str(datetime.fromtimestamp(self.current_time)),
                )
                self.web_data = json.loads(r.text, strict=False)
                time.sleep(0.25)
                if (status % 2) == 0:
                    self.current_time = self.current_time + (60 * 60 * 2)
                    self.update_url()
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
                            self.comments = append_row(self.comments, post_data)
                        self.current_time = post_data["created_utc"]
                    except KeyboardInterrupt:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.comments = append_row(self.comments, post_data)
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
                            self.submissions = append_row(self.submissions, post_data)
                        self.current_time = post_data["created_utc"]
                    except KeyboardInterrupt:
                        if self.oversized:
                            self.csv_writer.writerow(list(post_data.values()))
                        else:
                            self.submissions = append_row(self.submissions, post_data)
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
                            print("Keyboard Interrupt Detected, your object's values are secure")
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
                            print("Keyboard Interrupt Detected, your object's values are secure")
                            break

        collect_submissions(self=self)
        collect_comments(self=self)
        try:
            if self.oversized:
                self.csv.close()
            self.df = pd.read_csv(self.write_path, low_memory=False)
        except:
            self.df = pd.concat([self.submissions, self.comments], ignore_index=True)

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


__all__ = ["pushshift_file_query", "pushshift_web_query", "query"]

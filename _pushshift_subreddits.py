from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from _pushshift_api import api_agent


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


__all__ = ["subreddits"]

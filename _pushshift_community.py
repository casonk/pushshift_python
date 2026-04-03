import re
from dataclasses import dataclass

import numpy as np
import pandas as pd

from _pushshift_scalers import boolean_scaler, numeric_scaler, string_scaler


@dataclass
class community:
    def __init__(
        self,
        name="community",
        path=None,
        dataframe=None,
        columns=None,
        file_format=None,
        set_id=True,
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
        if path is None:
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
        if set_id:
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
            self.text_urls.index = self.text_urls.index.str.replace(r"\\_", r"_", regex=True)
            self.text_url_references = pd.DataFrame(
                self.text_urls.index.str.extract(subreddit_pattern)[1].value_counts()
            )
            self.text_url_references.columns = ["count"]
            self.text_url_references.index.name = "url"

        if post_type is None:
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

        if post_type is None:
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
        total_submission_comments = self.df[type_mask].groupby("author")["num_comments"].sum()
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

        def make_gini(df, column, min_val=1):
            gini_base = df[column].loc[~(df[column] < (min_val))]
            users = gini_base.index
            n = len(users)
            if n == 0 or gini_base.sum() == 0:
                return 0
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
            gini_dict = {col + "_gini": make_gini(self.authors, col) for col in gini_cols}
        except:
            self.authors()
            gini_dict = {col + "_gini": make_gini(self.authors, col) for col in gini_cols}
        self.gini = pd.DataFrame(data=gini_dict, index=[self.name])

        return self.gini

    def simpson(self):
        """
        Calculate Simpson-Gini Index for Community Authors.
        """

        def make_simpson(df, column, min_val=1):
            simpson_base = df[column].loc[~(df[column] < (min_val))]
            if simpson_base.sum() <= 1:
                return 0
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
                col + "_simpson": make_simpson(self.authors, col) for col in simpson_cols
            }
        except:
            self.authors()
            simpson_dict = {
                col + "_simpson": make_simpson(self.authors, col) for col in simpson_cols
            }
        self.simpson = pd.DataFrame(data=simpson_dict, index=[self.name])

        return self.simpson

    def shannon(self):
        """
        Calculate Shannon Entropy for Community Authors.
        """

        def make_shannon(df, column, min_val=1):
            shannon_base = df[column].loc[(df[column] >= (min_val))]
            if shannon_base.sum() == 0:
                return 0
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
                col + "_shannon": make_shannon(self.authors, col) for col in shannon_cols
            }
        except:
            self.authors()
            shannon_dict = {
                col + "_shannon": make_shannon(self.authors, col) for col in shannon_cols
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
        drop=None,
    ):
        """
        Create feature dataframe.
        ----------
        paramaters
        ----------
        drop: DataFrame column to remove from feature anlysis.
        """

        if drop is None:
            drop = [
                "author_community_total_posts",
                "author_community_total_post_score",
                "url_ref",
                "body_ref",
                "url_label",
                "body_label",
            ]
        self.features = pd.DataFrame()
        self.features["post_type"] = self.df["post_type"].copy()
        self.features["post_type"] = self.features["post_type"].replace(
            {"submission": 1, "comment": 0}
        )
        self.features["year"] = (pd.to_datetime(self.df["datetime"]).dt.year - 2005) / 17
        self.features["day_of_year"] = pd.to_datetime(self.df["datetime"]).dt.dayofyear / 365
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
            "year",
            "score",
            "num_comments",
            "controversiality",
            "total_awards_received",
            "author_community_total_submissions",
            "author_community_total_submission_score",
            "author_community_total_submission_comments",
            "author_community_total_comments",
            "author_community_total_comment_score",
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

        self.features["url_label"] = self.features["url_ref"].apply(lambda x: ref_label(x))
        self.features["body_label"] = self.features["body_ref"].apply(lambda x: ref_label(x))
        self.features["label"] = self.features["url_label"] + self.features["body_label"]

        def label(val):
            if val > 0:
                return 1
            else:
                return 0

        self.features["label"] = self.features["label"].apply(lambda x: label(x))

        if drop:
            for feature in drop:
                self.features.drop(feature, axis=1, inplace=True)

        for feature in self.features.columns:
            if pd.api.types.is_numeric_dtype(self.features[feature]):
                self.features[feature] = self.features[feature].fillna(0)
            else:
                self.features[feature] = self.features[feature].fillna("")

        submission_mask = self.features["post_type"] == 1
        comment_mask = self.features["post_type"] == 0
        self.feature_submissions = self.features[submission_mask]
        self.feature_comments = self.features[comment_mask]

        return self.features


__all__ = ["community"]

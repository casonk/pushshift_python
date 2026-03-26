import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pushshift_python as psp


def _write_zst(path, posts):
    payload = "\n".join(json.dumps(post) for post in posts) + "\n"
    with open(path, "wb") as handle:
        compressor = zstandard.ZstdCompressor()
        handle.write(compressor.compress(payload.encode("utf-8")))


def test_import_helpers_handle_current_pandas_behavior():
    df = pd.DataFrame(
        {
            "flag": [True, False],
            "constant": [5, 5],
            "value": ["a", None],
        }
    )

    scaled_bool = psp.boolean_scaler("flag", df)
    scaled_num = psp.numeric_scaler("constant", df)
    scaled_str = psp.string_scaler("value", df)

    assert scaled_bool.tolist() == [1, 0]
    assert scaled_num.tolist() == [0.0, 0.0]
    assert scaled_str.notna().sum() == 2


def test_public_api_reexports_core_symbols():
    expected = {
        "api_agent",
        "community",
        "file_handler",
        "modeling",
        "pushshift_file_query",
        "pushshift_web_query",
        "query",
        "subreddits",
    }

    for symbol in expected:
        assert hasattr(psp, symbol)


def test_file_query_collects_comment_and_submission_records(tmp_path):
    comment_dir = tmp_path / "comments"
    submission_dir = tmp_path / "submissions"
    comment_dir.mkdir()
    submission_dir.mkdir()

    after = 1609459200
    before = 1609545600

    submission_post = {
        "subreddit": "python",
        "id": "sub_1",
        "parent_id": "t3_root",
        "link_id": "t3_root",
        "url": "https://example.com",
        "permalink": "/r/python/comments/sub_1/example",
        "created_utc": after + 10,
        "score": 10,
        "upvote_ratio": 0.9,
        "num_comments": 2,
        "controversiality": 0,
        "total_awards_received": 0,
        "stickied": False,
        "post_hint": "link",
        "is_self": False,
        "is_video": False,
        "title": "Example submission",
        "selftext": "submission body",
        "author": "submitter",
        "author_premium": False,
    }
    comment_post = {
        "subreddit": "python",
        "id": "com_1",
        "parent_id": "t3_sub_1",
        "link_id": "t3_sub_1",
        "permalink": "/r/python/comments/sub_1/comment",
        "created_utc": after + 20,
        "score": 3,
        "controversiality": 0,
        "total_awards_received": 0,
        "stickied": False,
        "is_self": False,
        "is_video": False,
        "body": "comment body",
        "author": "commenter",
        "author_premium": True,
    }

    _write_zst(submission_dir / "RS_2021-01.zst", [submission_post])
    _write_zst(comment_dir / "RC_2021-01.zst", [comment_post])

    query = psp.pushshift_file_query(
        "subreddit",
        "python",
        {"after": after, "before": before},
    )
    query.set_parent_folders(submission_dir, comment_dir)
    query.make_query()

    assert len(query.submissions) == 1
    assert len(query.comments) == 1
    assert set(query.df["post_type"]) == {"submission", "comment"}


def test_features_keep_submission_and_comment_splits_aligned():
    df = pd.DataFrame(
        [
            {
                "post_type": "submission",
                "subreddit": "python",
                "id": "sub_1",
                "parent_id": "nan",
                "link_id": "nan",
                "url": "https://reddit.com/r/python/",
                "permalink": "/r/python/comments/sub_1/example",
                "created_utc": 1609459200,
                "datetime": "01/01/2021",
                "score": 10,
                "upvote_ratio": 0.9,
                "num_comments": 2,
                "controversiality": 0,
                "total_awards_received": 0,
                "stickied": False,
                "post_hint": "link",
                "is_self": False,
                "is_video": False,
                "title": "Visit r/python",
                "body": "See r/learnpython too",
                "author": "alice",
                "author_premium": False,
            },
            {
                "post_type": "comment",
                "subreddit": "python",
                "id": "com_1",
                "parent_id": "t3_sub_1",
                "link_id": "t3_sub_1",
                "url": "nan",
                "permalink": "/r/python/comments/sub_1/comment",
                "created_utc": 1609462800,
                "datetime": "01/01/2021",
                "score": 1,
                "upvote_ratio": np.nan,
                "num_comments": np.nan,
                "controversiality": 0,
                "total_awards_received": 0,
                "stickied": False,
                "post_hint": "nan",
                "is_self": False,
                "is_video": False,
                "title": "nan",
                "body": "r/python is useful",
                "author": "bob",
                "author_premium": True,
            },
        ]
    )

    community = psp.community(name="python", dataframe=df)
    community.features(drop=False)

    assert set(community.feature_submissions["post_type"]) == {1}
    assert set(community.feature_comments["post_type"]) == {0}


def test_modeling_uses_continuous_scores_for_curves():
    df = pd.DataFrame(
        {
            "feature_a": [0, 1, 0, 1, 0, 1, 0, 1],
            "feature_b": [0.1, 0.9, 0.2, 0.8, 0.15, 0.85, 0.05, 0.95],
            "label": [0, 1, 0, 1, 0, 1, 0, 1],
        }
    )

    model = psp.modeling(df, rs=0, timeseries=True)
    model.linear_regression(mi=500)

    assert "lr" in model.prediction_scores
    assert len(model.prediction_scores["lr"]) == len(model.y_test)
    assert model.prediction_scores["lr"].dtype.kind == "f"

import pandas as pd 
import zstandard
from datetime import datetime
import json
import csv
import os

comm_dir = "/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/RC/"
subm_dir = "/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/RS/"

_end_dates    = pd.Series(pd.date_range(start="2020-12-12", end="2021-01-31", freq="D", tz='America/New_York'))
_start_dates  = _end_dates - pd.Timedelta(days=7)
_center_dates = _end_dates - pd.Timedelta(days=3.5)
end_dates     = _end_dates.apply(lambda x: x.timestamp())
center_dates  = _center_dates.apply(lambda x: x.timestamp())
start_dates   = _start_dates.apply(lambda x: x.timestamp())
_end_dates    = _end_dates.apply(lambda x:str(x).split(' ')[0])
_center_dates = _center_dates.apply(lambda x:str(x).split(' ')[0])
_start_dates  = _start_dates.apply(lambda x:str(x).split(' ')[0])

def read_lines_zst(fh):
    with open(fh, "rb") as file_handle:
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

id_l = '/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL'

errors = 0
lines = 0
skipped = 0

def create_common_data(post):
    """
    Helper function to collect values common between both comments and submissions.
    """

    try:
        subreddit = post["subreddit"]
        post_id = post["id"]
        try:
            link_id = post["link_id"]
        except KeyError:
            link_id = "nan"
        created_utc = post["created_utc"]
        author = post["author"]
        author = r"{}".format(author)

        post_data = {
            "author": author,
            "id": post_id,
            "link_id": link_id[3:],
            "subreddit": subreddit,
            "created_utc": created_utc,
        }
        return post_data
    except KeyboardInterrupt:
        pass

#####################################################

headers = ["Author", "Id", "Link_Id", "Subreddit", 'UTC']
with open((id_l + '\\' + 'ID_PAIRS.csv'), "w+", newline="", encoding="utf-8") as _out:
    csv_writer = csv.writer(_out, delimiter=",", escapechar="\\")
    csv_writer.writerow(headers)

    os.chdir(comm_dir)
    for file in os.listdir():
        for line, file_bytes_processed in read_lines_zst(file):
            try:
                _post = json.loads(line)
                post_data = create_common_data(post=_post)
                csv_writer.writerow(
                        [post_data['author'], post_data['id'], post_data['link_id'], post_data['subreddit'], post_data['created_utc']]
                    )
            except (KeyError, json.JSONDecodeError):
                errors += 1
            
            lines += 1
            if (lines + 1) % 100001 == 0:
                print(lines, 'comment lines processed')
            if (skipped + 1) % 100001 == 0:
                print(skipped, 'comment lines skipped')

    os.chdir(subm_dir)
    for file in os.listdir():
        for line, file_bytes_processed in read_lines_zst(file):
            try:
                _post = json.loads(line)
                post_data = create_common_data(post=_post)
                csv_writer.writerow(
                        [post_data['author'], post_data['id'], post_data['link_id'], post_data['subreddit'], post_data['created_utc']]
                    )
            except (KeyError, json.JSONDecodeError):
                errors += 1
            
            lines += 1
            if (lines + 1) % 100001 == 0:
                print(lines, 'comment lines processed')
            if (skipped + 1) % 100001 == 0:
                print(skipped, 'comment lines skipped')


print('comment complete, errors =', errors)

print('skipped total:', skipped, 'lines total:', lines, 'errors total:', errors)
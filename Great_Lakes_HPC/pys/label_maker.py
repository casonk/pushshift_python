#!/sw/arcts/centos7/python3.8-anaconda/2021.05/bin/python
 
# Load  Python  modules #
#|||||||||||||||||||||||#

import os
import pandas as pd
import re

os.chdir('/scratch/mmani_root/mmani0/shared_data/hot/csvz/')
for file in os.listdir():
    if file.endswith('.csv'):
        print(file)
        try:
            comm = pd.read_csv(filepath_or_buffer=file, index_col=False, low_memory=False,)
            comm.set_index("id", inplace=True)

            subreddit_pattern = re.compile(r"(.*reddit.com/r/)([\w]+)(/.*)")
            find_r_slash_refs = re.compile(r"(.*\br/)([\w]+)(\b.*)")

            comm.url_ref = comm["url"].str.extract(subreddit_pattern)[1]
            comm.url_ref = comm.url_ref.str.lower()

            comm.body_ref = comm["body"].str.extract(find_r_slash_refs)[1]
            comm.body_ref = comm.body_ref.str.lower()

            comm.title_ref = comm["title"].str.extract(find_r_slash_refs)[1]
            comm.title_ref = comm.title_ref.str.lower()

            label_data = pd.DataFrame()
            label_data['utc'] = comm.df.created_utc
            label_data['author'] = comm.df.author
            label_data['subreddit'] = comm.df.subreddit
            label_data['url_ref'] = comm.url_ref
            label_data['body_ref'] = comm.body_ref
            label_data['title_ref'] = comm.title_ref
            label_data.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_labelz/label_'+file)
        except Exception as e:
            continue     

exit()
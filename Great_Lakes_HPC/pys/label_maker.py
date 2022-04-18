#!/sw/arcts/centos7/python3.8-anaconda/2021.05/bin/python
 
# Load  Python  modules #
#|||||||||||||||||||||||#

import os
import pandas as pd
import re

def ref_list(x):
    refs = []
    for i in range(len(x)):
        if pd.isna(x[i]):
            continue
        else:
            refs += [x[i]]
    return refs

os.chdir('/scratch/mmani_root/mmani0/shared_data/hot/csvz/')
for file in os.listdir():
    if file.endswith('.csv'):
        print(file)
        try:
            comm = pd.read_csv(filepath_or_buffer=file, index_col=False, low_memory=False,)
            comm.set_index("id", inplace=True)
            comm = comm.iloc[:5000,:]
            subreddit_pattern = re.compile(r"(.*reddit.com/r/)([\w]+)(/.*)")
            find_r_slash_refs = re.compile(r"(.*\br/)([\w]+)(\b.*)")

            comm["url"] = comm["url"].str.lower()
            url_direct_ref = pd.DataFrame(comm["url"].str.extractall(subreddit_pattern)[1]).unstack().droplevel(level=0, axis=1)
            url_direct_ref = url_direct_ref.apply(lambda x: ref_list(x), axis=1)

            comm["body"] = comm["body"].str.lower()
            body_direct_ref = pd.DataFrame(comm["body"].str.extractall(subreddit_pattern)[1]).unstack().droplevel(level=0, axis=1)
            body_direct_ref = body_direct_ref.apply(lambda x: ref_list(x), axis=1)
            body_indirect_ref = pd.DataFrame(comm["body"].str.extractall(find_r_slash_refs)[1]).unstack().droplevel(level=0, axis=1)
            body_indirect_ref = body_indirect_ref.apply(lambda x: ref_list(x), axis=1)

            comm["title"] = comm["title"].str.lower()
            title_indirect_ref = pd.DataFrame(comm["title"].str.extractall(find_r_slash_refs)[1]).unstack().droplevel(level=0, axis=1)
            title_indirect_ref = title_indirect_ref.apply(lambda x: ref_list(x), axis=1)

            label_data = pd.DataFrame()
            label_data['utc'] = comm.created_utc
            label_data['author'] = comm.author
            label_data['subreddit'] = comm.subreddit.str.lower()
            label_data['url_direct_ref'] = url_direct_ref 
            label_data['url_direct_ref'] = label_data['url_direct_ref'].fillna("").apply(list)
            label_data['body_direct_ref'] = body_direct_ref 
            label_data['body_direct_ref'] = label_data['body_direct_ref'].fillna("").apply(list)
            label_data['body_indirect_ref'] = body_indirect_ref
            label_data['body_indirect_ref'] = label_data['body_indirect_ref'].fillna("").apply(list)
            label_data['title_indirect_ref'] = title_indirect_ref
            label_data['title_indirect_ref'] = label_data['title_indirect_ref'].fillna("").apply(list)
            label_data['refs'] = label_data['url_direct_ref'] + label_data['body_direct_ref'] + label_data['body_indirect_ref'] + label_data['title_indirect_ref']
            label_data.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_labelz/label_' + file)
        except Exception as e:
            print('exception as :', e)
            continue     

exit()
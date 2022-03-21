import pushshift_python as push_py
import pandas as pd
import traceback
import logging
import datetime
import os

start = datetime.datetime.now()
logging.basicConfig(filename='<Target>\\'+str(start.date())+'<log>.log', encoding='utf-8', level=logging.DEBUG)

os.chdir('<Source>')
for file in os.listdir():
    if file.endswith('.csv'):
        print(file)
        try:
            comm  = push_py.community(path=file, file_format='csv')
            comm.labels()
            label_data = pd.DataFrame()
            label_data['utc'] = comm.df.created_utc
            label_data['author'] = comm.df.author
            label_data['subreddit'] = comm.df.subreddit
            label_data['url_ref'] = comm.url_ref
            label_data['body_ref'] = comm.body_ref
            label_data['title_ref'] = comm.title_ref
            label_data.to_csv('<Target>\\label_'+file)
        except Exception as e:
            logging.error(traceback.format_exc())
            continue     

end = datetime.datetime.now()
logging.info(msg=('started', start))
logging.info(msg=('ended', end))
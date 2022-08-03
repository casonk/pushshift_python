import pandas as pd 
import os


_end_dates    = pd.Series(pd.date_range(start="2020-10-08", end="2021-03-31", freq="D", tz='America/New_York'))
_start_dates  = _end_dates - pd.Timedelta(days=7)
_center_dates = _end_dates - pd.Timedelta(days=3.5)
end_dates     = _end_dates.apply(lambda x: x.timestamp())
center_dates  = _center_dates.apply(lambda x: x.timestamp())
start_dates   = _start_dates.apply(lambda x: x.timestamp())
_end_dates    = _end_dates.apply(lambda x:str(x).split(' ')[0])
_center_dates = _center_dates.apply(lambda x:str(x).split(' ')[0])
_start_dates  = _start_dates.apply(lambda x:str(x).split(' ')[0])


id_l = '/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/'
df = pd.read_csv((id_l + 'UTIL/ID_PAIRS.csv'), low_memory=False)
df.to_pickle((id_l + 'UTIL/ID_PAIRS.pkl'))


for date in _center_dates:
    try:
        os.mkdir(id_l + '/' + date + '/')
    except:
        pass


for i in range(len(start_dates)):
    min_mask = df['UTC'].astype('int64') >= start_dates[i]
    max_mask = df['UTC'].astype('int64') <= end_dates[i]
    tmp_df = df[min_mask & max_mask]
    tmp_df.to_pickle((id_l + _center_dates[i] + '/UTIL/ID_PAIRS.pkl'))
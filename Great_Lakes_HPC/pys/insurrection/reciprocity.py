import networkx as nx
import pandas as pd
import pickle

pd.set_option('display.max_rows',10000)
pd.set_option('display.min_rows',2000)
pd.set_option('display.column_space',30)
pd.set_option('display.max_colwidth',150)
pd.set_option('display.expand_frame_repr',True)

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

j=1
k=21
r=5


with open('/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/2020-10-04/LC_1_21_10.pkl', 'rb') as fh:
    aus = pickle.load(fh)

with open('/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/2020-10-04/G_1_21.pkl', 'rb') as fh:
    G = pickle.load(fh)

print(nx.overall_reciprocity(G))

print(nx.overall_reciprocity(G), aus[0])

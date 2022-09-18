import pandas as pd
import networkx as nx
from networkx.algorithms import community
import pickle
import os

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

j=1
k=21
keys = []
id_l = '/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/'

for i in range(len(start_dates)):
    trimmed_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))

    dG = nx.DiGraph()
    for info in trimmed_df.values:
        dG.add_edge(info[0], info[1], subreddit=info[2], weight=int(info[3]))
    print(date, j, k)
    
    with open((id_l + date + ('/dG_{}_{}.pkl').format(j, k)), 'wb') as Gh:
        pickle.dump(dG, Gh)

def ecu(r):
    for date in _center_dates:
        if os.path.isfile((id_l + date + ('/dLC_{}_{}_{}.pkl').format(j, k, r))):
            print('pass lc:', date, r)
            continue
        with open((id_l + date + ('/dG_{}_{}.pkl').format(j, k)), 'rb') as Gh:
            dG = pickle.load(Gh)

        lc = community.louvain_communities(G, weight='weight', resolution=r, threshold=1e-07, seed=123)
        print(date, len(lc), r)

        with open((id_l + date + ('/dLC_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lch:
            pickle.dump(lc, lch)

# ecu(0.5)
# ecu(1)
# ecu(2)
ecu(5)
ecu(10)

print()
exit()
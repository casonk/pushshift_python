import pandas as pd
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

id_l = '/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/'

j=1
k=21
sw = 2/9
aw = 7/9

def olapper(r):
    sub_sets = {}
    for date in _center_dates:
        sub_sets[date] = {}
        with open((id_l + date + ('/SUB_SETS_{}_{}_{}.pkl').format(j, k, r)), 'rb') as ssh:
            sub_sets[date] = pickle.load(ssh)
        print(date, 'SUB_SET')

    auth_sets = {}
    for date in _center_dates:
        auth_sets[date] = {}
        with open((id_l + date + ('/AUTH_SETS_{}_{}_{}.pkl').format(j, k, r)), 'rb') as ash:
            auth_sets[date] = pickle.load(ash)
        print(date, 'AUTH_SET')

    olap = {}
    for date1 in _center_dates:
        olap[date1] = {}
        for date2 in _center_dates:
            olap[date1][date2] = {}
            for set1 in sub_sets[date1]:
                olap[date1][date2][set1] = {}
                for set2 in sub_sets[date2]:
                    olap[date1][date2][set1][set2] = (len(sub_sets[date1][set1] & sub_sets[date2][set2]) / len(sub_sets[date1][set1]) * sw)
            print(date1, date2, 'SLAPPED')

    for date1 in _center_dates:
        for date2 in _center_dates:
            for set1 in auth_sets[date1]:
                for set2 in auth_sets[date2]:
                    olap[date1][date2][set1][set2] += (len(auth_sets[date1][set1] & auth_sets[date2][set2]) / len(auth_sets[date1][set1]) * aw)
            print(date1, date2, 'ALAPPED')

    for date1 in _center_dates:
        for date2 in _center_dates:
            for set1 in sub_sets[date1]:
                max_olap = 0
                best_set = 0
                for set2 in sub_sets[date2]:
                    if max_olap < olap[date1][date2][set1][set2]:
                        max_olap = olap[date1][date2][set1][set2]
                        best_set = set2
                olap[date1][date2][set1] = best_set
            print(date1, date2, 'OLAPPED')

    with open((id_l + ('/UTIL/OLAP_{}_{}_{}.pkl').format(j, k, r)), 'wb') as olh:
        pickle.dump(olap, olh)

    print(r, 'OLAP PICKLED')
    

# olapper(0.5)
# olapper(1)
# olapper(2)
# olapper(5)
olapper(10)

print()
exit()
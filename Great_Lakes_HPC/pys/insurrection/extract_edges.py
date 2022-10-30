import pandas as pd 


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

# for i in range(len(start_dates)):
#     out_name = (id_l + _center_dates[i] + '/EDGE_LIST_RAW.pkl')
#     df = pd.read_pickle((id_l + _center_dates[i] + '/UTIL/ID_PAIRS.pkl'))
#     df.drop('UTC', axis=1, inplace=True)
#     df.set_index('Id', inplace=True)
#     print(_center_dates[i], 'loaded')


#     link_mask = df['Link_Id'].isna()
#     link_ids = df['Link_Id'][~link_mask]
#     id_mask = link_ids.isin(df.index)
#     link_ids = link_ids[id_mask]
#     auth_mask = df['Link_Id'].isin(link_ids)

#     Source = df[auth_mask]['Author']
#     Subreddit = df[auth_mask]['Subreddit']
#     ref_frame = df.loc[link_ids]
#     Target = ref_frame['Author']
#     Target.index = range(len(Target))
#     Source.index = range(len(Source))
#     Subreddit.index = range(len(Subreddit))
#     edge_list = pd.DataFrame(data={'Source':Source, 'Target':Target, 'Subreddit':Subreddit}, index=range(len(Source)), columns=['Source','Target','Subreddit'])

#     edge_list['Source'] = edge_list['Source'].str.lower()
#     edge_list['Target'] = edge_list['Target'].str.lower()
#     edge_list['Subreddit'] = edge_list['Subreddit'].str.lower()
#     print(_center_dates[i], 'framed')


#     edge_list.to_pickle(out_name)
#     print(_center_dates[i], 'pickled')


#     self_mask = edge_list['Source'] != edge_list['Target']
#     selfless_edge_list = edge_list[self_mask]
#     print(_center_dates[i], 'now selfless')


#     selfless_edge_list.to_pickle((id_l + _center_dates[i] + '/EDGE_LIST_SELFLESS.pkl'))
#     print(_center_dates[i], 'repickled\n')

def raw_thresher(net, min_user_posts, min_sub_posts):
    an = net[net >= min_user_posts].reset_index().rename(columns={0:'Count'})
    print(_center_dates[i], min_user_posts, min_sub_posts)
    
    asubs_mask = an['Subreddit'].value_counts() >= min_sub_posts
    asubs = an['Subreddit'].value_counts()[asubs_mask].index.to_series()
    smask = an['Subreddit'].isin(asubs)
    an[smask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW_{}_{}.pkl'.format(min_user_posts,min_sub_posts))))

def unraw_thresher(net, max_user_posts, min_sub_posts):
    an = net[net < max_user_posts].reset_index().rename(columns={0:'Count'})
    print(_center_dates[i], max_user_posts, min_sub_posts)
    
    asubs_mask = an['Subreddit'].value_counts() >= min_sub_posts
    asubs = an['Subreddit'].value_counts()[asubs_mask].index.to_series()
    smask = an['Subreddit'].isin(asubs)
    an[smask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW_{}_{}.pkl'.format(max_user_posts,min_sub_posts))))

def inraw_thresher(net, min_user_posts, max_user_posts, min_sub_posts):
    an = net[(net >= min_user_posts) & (net < max_user_posts)].reset_index().rename(columns={0:'Count'})
    print(_center_dates[i], min_user_posts, max_user_posts, min_sub_posts)
    
    asubs_mask = an['Subreddit'].value_counts() < min_sub_posts
    asubs = an['Subreddit'].value_counts()[asubs_mask].index.to_series()
    smask = an['Subreddit'].isin(asubs)
    an[smask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW_{}_{}_{}.pkl'.format(min_user_posts,max_user_posts,min_sub_posts))))

def raw_counter(net, min_user_posts):
    src_counts = net.groupby('Source').sum()
    src_users = src_counts[(src_counts >= min_user_posts)].index.to_series()
    tgt_counts = net.groupby('Target').sum()
    tgt_users = tgt_counts[(tgt_counts >= min_user_posts)].index.to_series()
    users = pd.concat([src_users, tgt_users]).unique()
    
    an = net.reset_index().rename(columns={0:'Count'})
    src_mask = an['Source'].isin(users)
    tgr_mask = an['Target'].isin(users)
    an[src_mask | tgr_mask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW__{}.pkl'.format(min_user_posts))))
    print(_center_dates[i], min_user_posts)
    
def unraw_counter(net, max_user_posts):
    src_counts = net.groupby('Source').sum()
    src_users = src_counts[(src_counts <= max_user_posts)].index.to_series()
    tgt_counts = net.groupby('Target').sum()
    tgt_users = tgt_counts[(tgt_counts <= max_user_posts)].index.to_series()
    users = pd.concat([src_users, tgt_users]).unique()
    
    an = net.reset_index().rename(columns={0:'Count'})
    src_mask = an['Source'].isin(users)
    tgr_mask = an['Target'].isin(users)
    an[src_mask | tgr_mask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW_{}__.pkl'.format(max_user_posts))))
    print(_center_dates[i], max_user_posts)
    
def inraw_counter(net, min_user_posts, max_user_posts):
    src_counts = net.groupby('Source').sum()
    src_users = src_counts[(src_counts >= min_user_posts) & (src_counts <= max_user_posts)].index.to_series()
    tgt_counts = net.groupby('Target').sum()
    tgt_users = tgt_counts[(tgt_counts >= min_user_posts) & (tgt_counts <= max_user_posts)].index.to_series()
    users = pd.concat([src_users, tgt_users]).unique()

    an = net.reset_index().rename(columns={0:'Count'})
    src_mask = an['Source'].isin(users)
    tgr_mask = an['Target'].isin(users)
    an[src_mask | tgr_mask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_RAW__{}_{}.pkl'.format(min_user_posts,max_user_posts))))
    print(_center_dates[i], min_user_posts, max_user_posts)
    
for i in range(len(start_dates)):
    # selfless_edge_list = pd.read_pickle((id_l + _center_dates[i] + '/EDGE_LIST_SELFLESS.pkl'))
    # edge_list = pd.read_pickle((id_l + _center_dates[i] + '/EDGE_LIST_RAW.pkl'))
    # auth_net = selfless_edge_list.value_counts()
    # auth_net = edge_list.value_counts()
    # auth_net.to_pickle((id_l + _center_dates[i] + '/SELFLESS_AUTHOR_NET.pkl'))
    # auth_net.to_pickle((id_l + _center_dates[i] + '/AUTHOR_NET.pkl'))
    # auth_net = pd.read_pickle((id_l + _center_dates[i] + '/SELFLESS_AUTHOR_NET.pkl'))
    auth_net = pd.read_pickle((id_l + _center_dates[i] + '/AUTHOR_NET.pkl'))
    

    # inraw_counter(auth_net, 1, 6)
    # inraw_counter(auth_net, 2, 7)
    # inraw_counter(auth_net, 3, 8)
    # inraw_counter(auth_net, 4, 9)
    # inraw_counter(auth_net, 5, 10)
    # inraw_counter(auth_net, 6, 11)
    # inraw_counter(auth_net, 7, 12)
    # inraw_counter(auth_net, 8, 13)
    # inraw_counter(auth_net, 9, 14)
    # inraw_counter(auth_net, 10, 15)
    # inraw_counter(auth_net, 15, 20)

    # unraw_counter(auth_net, 1)
    # unraw_counter(auth_net, 2)
    # unraw_counter(auth_net, 3)
    # unraw_counter(auth_net, 4)
    # unraw_counter(auth_net, 5)
    # unraw_counter(auth_net, 6)
    # unraw_counter(auth_net, 7)
    # unraw_counter(auth_net, 8)
    # unraw_counter(auth_net, 9)
    # unraw_counter(auth_net, 10)
    # unraw_counter(auth_net, 15)
    # unraw_counter(auth_net, 20)

    # raw_counter(auth_net, 20)
    # raw_counter(auth_net, 15)
    # raw_counter(auth_net, 10)
    # raw_counter(auth_net, 9)
    # raw_counter(auth_net, 8)
    # raw_counter(auth_net, 7)
    # raw_counter(auth_net, 6)
    # raw_counter(auth_net, 5)
    # raw_counter(auth_net, 3)
    # raw_counter(auth_net, 4)
    # raw_counter(auth_net, 2)
    # raw_counter(auth_net, 1)


    # inraw_thresher(auth_net, 1, 6, 0)
    # inraw_thresher(auth_net, 2, 7, 0)
    # inraw_thresher(auth_net, 3, 8, 0)
    # inraw_thresher(auth_net, 4, 9, 0)
    # inraw_thresher(auth_net, 5, 10, 0)
    # inraw_thresher(auth_net, 6, 11, 0)
    # inraw_thresher(auth_net, 7, 12, 0)
    # inraw_thresher(auth_net, 8, 13, 0)
    # inraw_thresher(auth_net, 9, 14, 0)
    # inraw_thresher(auth_net, 10, 15, 0)

    # unraw_thresher(auth_net, 2, 0)
    # unraw_thresher(auth_net, 3, 0)
    # unraw_thresher(auth_net, 4, 0)
    # unraw_thresher(auth_net, 5, 0)
    # unraw_thresher(auth_net, 6, 0)
    # unraw_thresher(auth_net, 7, 0)
    # unraw_thresher(auth_net, 8, 0)
    # unraw_thresher(auth_net, 9, 0)
    # unraw_thresher(auth_net, 10, 0)
    # unraw_thresher(auth_net, 15, 0)
    # unraw_thresher(auth_net, 20, 0)

    # raw_thresher(auth_net, 20, 200)
    # raw_thresher(auth_net, 15, 100)
    # raw_thresher(auth_net, 10, 50)
    # raw_thresher(auth_net, 9, 45)
    # raw_thresher(auth_net, 8, 40)
    # raw_thresher(auth_net, 7, 32)
    # raw_thresher(auth_net, 6, 27)
    # raw_thresher(auth_net, 5, 25)
    # raw_thresher(auth_net, 3, 22)
    # raw_thresher(auth_net, 4, 20)
    # raw_thresher(auth_net, 2, 20)
    # raw_thresher(auth_net, 1, 15)

    # j=1 
    # k=21

    # an = auth_net[auth_net >= j].reset_index().rename(columns={0:'Count'})
    # print(_center_dates[i], j, k)
    
    # asubs_mask = an['Subreddit'].value_counts() >= k
    # asubs = an['Subreddit'].value_counts()[asubs_mask].index.to_series()
    # smask = an['Subreddit'].isin(asubs)
    # an[smask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_SELFLESS_{}_{}.pkl'.format(j,k))))

    # j=2
    # k=21

    # an = auth_net[auth_net >= j].reset_index().rename(columns={0:'Count'})
    # print(_center_dates[i], j, k)
    
    # asubs_mask = an['Subreddit'].value_counts() >= k
    # asubs = an['Subreddit'].value_counts()[asubs_mask].index.to_series()
    # smask = an['Subreddit'].isin(asubs)
    # an[smask].to_pickle((id_l + _center_dates[i] + ('/EDGE_LIST_SELFLESS_{}_{}.pkl'.format(j,k))))

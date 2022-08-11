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
keys = []

r=0.5
for date in _center_dates:
    i = 0
    level_map = {}
    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'rb') as lch:
        lc = pickle.load(lch)

    communities_series = pd.Series([len(sett) for sett in lc])
    communities_series.to_pickle(id_l + date + ('/COMMUNITIES_SERIES_{}_{}_{}.pkl').format(j, k, r))

    for sett in lc:
        for auth in sett:
            level_map[auth] = i
        i += 1

    with open((id_l + date + ('/LEVEL_MAP_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lmh:
        pickle.dump(level_map, lmh)
        print(r, date, 'LEVEL_MAPPED')

    level_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    level_df['Source'] = level_df['Source'].apply(lambda auth: level_map[auth])
    level_df['Target'] = level_df['Target'].apply(lambda auth: level_map[auth])
    level_df.to_pickle(id_l + date + ('/LEVEL_DF_{}_{}_{}.pkl').format(j, k, r))

    maximalist_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    maximalist_df['Source_Level'] = level_df['Source']
    maximalist_df['Target_Level'] = level_df['Target']
    maximalist_df.to_pickle((id_l + date + ('/MAXIMALIST_DF_{}_{}_{}.pkl').format(j, k, r)))

    counters = pd.DataFrame(maximalist_df[['Source_Level', 'Target_Level', 'Count']].groupby('Source_Level').value_counts()).unstack().droplevel(0, axis=1).fillna(0)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters[1] = counters[1] + (counters[col] * col)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters.drop(col, axis=1, inplace=True)

    inlinks = {}
    outlinks = {}
    for pair in counters.index:
        source, target = pair
        if source == target:
            inlinks[source] = counters.loc[pair]
        else:
            outlinks[source] = outlinks.get(source, 0) + counters.loc[pair]

    Internal_Links = [inlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    External_Links = [outlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    ei_df = pd.DataFrame({'Internal_Links': Internal_Links, 'External_Links': External_Links})
    ei_df['Total_Links'] = (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df['EI_Index'] = (ei_df['External_Links'] - ei_df['Internal_Links']) / (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df.to_pickle((id_l + date + ('/EI_DF_{}_{}_{}.pkl').format(j, k, r)))

    comm_sub_pairs = maximalist_df[['Source_Level','Subreddit','Count']].groupby(['Source_Level','Subreddit']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_sub_pairs.to_pickle((id_l + date + ('/CSP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CSPPED')

    communities_series
    num_subreddits = comm_sub_pairs.groupby(level=0, axis=0).apply(lambda x : len(x))
    communities_df = pd.DataFrame({'Unique_Authors': communities_series, 'Unique_Subreddits': num_subreddits})
    communities_df.to_pickle(id_l + date + ('/COMMUNITIES_DF_{}_{}_{}.pkl').format(j, k, r))

    comm_auth_pairs= maximalist_df[['Source_Level','Source','Count']].groupby(['Source_Level','Source']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_auth_pairs.to_pickle((id_l + date + ('/CAP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CAPPED')


    top_c_dict = ei_df.sort_values(by=['Total_Links'], ascending=False).head(75)

    sub_sets = {}
    csp = comm_sub_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(70)).droplevel(0)
    for community in top_c_dict.index:
        sub_sets[community] = set(csp.loc[community].index)

    with open((id_l + date + ('/SUB_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ssh:
        pickle.dump(sub_sets, ssh)
        print(r, date, 'SUBSETT')

    auth_sets = {}
    cap = comm_auth_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(1500)).droplevel(0)
    for community in top_c_dict.index:
        auth_sets[community] = set(cap.loc[community].index)

    with open((id_l + date + ('/AUTH_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ash:
        pickle.dump(sub_sets, ash)
        print(r, date, 'AUTHSETT')

r=1
for date in _center_dates:
    i = 0
    level_map = {}
    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'rb') as lch:
        lc = pickle.load(lch)

    communities_series = pd.Series([len(sett) for sett in lc])
    communities_series.to_pickle(id_l + date + ('/COMMUNITIES_SERIES_{}_{}_{}.pkl').format(j, k, r))

    for sett in lc:
        for auth in sett:
            level_map[auth] = i
        i += 1

    with open((id_l + date + ('/LEVEL_MAP_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lmh:
        pickle.dump(level_map, lmh)
        print(r, date, 'LEVEL_MAPPED')

    level_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    level_df['Source'] = level_df['Source'].apply(lambda auth: level_map[auth])
    level_df['Target'] = level_df['Target'].apply(lambda auth: level_map[auth])
    level_df.to_pickle(id_l + date + ('/LEVEL_DF_{}_{}_{}.pkl').format(j, k, r))

    maximalist_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    maximalist_df['Source_Level'] = level_df['Source']
    maximalist_df['Target_Level'] = level_df['Target']
    maximalist_df.to_pickle((id_l + date + ('/MAXIMALIST_DF_{}_{}_{}.pkl').format(j, k, r)))

    counters = pd.DataFrame(maximalist_df[['Source_Level', 'Target_Level', 'Count']].groupby('Source_Level').value_counts()).unstack().droplevel(0, axis=1).fillna(0)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters[1] = counters[1] + (counters[col] * col)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters.drop(col, axis=1, inplace=True)

    inlinks = {}
    outlinks = {}
    for pair in counters.index:
        source, target = pair
        if source == target:
            inlinks[source] = counters.loc[pair]
        else:
            outlinks[source] = outlinks.get(source, 0) + counters.loc[pair]

    Internal_Links = [inlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    External_Links = [outlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    ei_df = pd.DataFrame({'Internal_Links': Internal_Links, 'External_Links': External_Links})
    ei_df['Total_Links'] = (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df['EI_Index'] = (ei_df['External_Links'] - ei_df['Internal_Links']) / (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df.to_pickle((id_l + date + ('/EI_DF_{}_{}_{}.pkl').format(j, k, r)))

    comm_sub_pairs = maximalist_df[['Source_Level','Subreddit','Count']].groupby(['Source_Level','Subreddit']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_sub_pairs.to_pickle((id_l + date + ('/CSP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CSPPED')

    communities_series
    num_subreddits = comm_sub_pairs.groupby(level=0, axis=0).apply(lambda x : len(x))
    communities_df = pd.DataFrame({'Unique_Authors': communities_series, 'Unique_Subreddits': num_subreddits})
    communities_df.to_pickle(id_l + date + ('/COMMUNITIES_DF_{}_{}_{}.pkl').format(j, k, r))

    comm_auth_pairs= maximalist_df[['Source_Level','Source','Count']].groupby(['Source_Level','Source']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_auth_pairs.to_pickle((id_l + date + ('/CAP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CAPPED')


    top_c_dict = ei_df.sort_values(by=['Total_Links'], ascending=False).head(75)

    sub_sets = {}
    csp = comm_sub_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(70)).droplevel(0)
    for community in top_c_dict.index:
        sub_sets[community] = set(csp.loc[community].index)

    with open((id_l + date + ('/SUB_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ssh:
        pickle.dump(sub_sets, ssh)
        print(r, date, 'SUBSETT')

    auth_sets = {}
    cap = comm_auth_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(1500)).droplevel(0)
    for community in top_c_dict.index:
        auth_sets[community] = set(cap.loc[community].index)

    with open((id_l + date + ('/AUTH_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ash:
        pickle.dump(sub_sets, ash)
        print(r, date, 'AUTHSETT')

r=2
for date in _center_dates:
    i = 0
    level_map = {}
    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'rb') as lch:
        lc = pickle.load(lch)

    communities_series = pd.Series([len(sett) for sett in lc])
    communities_series.to_pickle(id_l + date + ('/COMMUNITIES_SERIES_{}_{}_{}.pkl').format(j, k, r))

    for sett in lc:
        for auth in sett:
            level_map[auth] = i
        i += 1

    with open((id_l + date + ('/LEVEL_MAP_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lmh:
        pickle.dump(level_map, lmh)
        print(r, date, 'LEVEL_MAPPED')

    level_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    level_df['Source'] = level_df['Source'].apply(lambda auth: level_map[auth])
    level_df['Target'] = level_df['Target'].apply(lambda auth: level_map[auth])
    level_df.to_pickle(id_l + date + ('/LEVEL_DF_{}_{}_{}.pkl').format(j, k, r))

    maximalist_df = pd.read_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))
    maximalist_df['Source_Level'] = level_df['Source']
    maximalist_df['Target_Level'] = level_df['Target']
    maximalist_df.to_pickle((id_l + date + ('/MAXIMALIST_DF_{}_{}_{}.pkl').format(j, k, r)))

    counters = pd.DataFrame(maximalist_df[['Source_Level', 'Target_Level', 'Count']].groupby('Source_Level').value_counts()).unstack().droplevel(0, axis=1).fillna(0)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters[1] = counters[1] + (counters[col] * col)

    for col in counters.columns:
        if col == 1:
            continue
        else:
            counters.drop(col, axis=1, inplace=True)

    inlinks = {}
    outlinks = {}
    for pair in counters.index:
        source, target = pair
        if source == target:
            inlinks[source] = counters.loc[pair]
        else:
            outlinks[source] = outlinks.get(source, 0) + counters.loc[pair]

    Internal_Links = [inlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    External_Links = [outlinks.get(int(comm), {1:0})[1] for comm in range(len(lc))]
    ei_df = pd.DataFrame({'Internal_Links': Internal_Links, 'External_Links': External_Links})
    ei_df['Total_Links'] = (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df['EI_Index'] = (ei_df['External_Links'] - ei_df['Internal_Links']) / (ei_df['External_Links'] + ei_df['Internal_Links'])
    ei_df.to_pickle((id_l + date + ('/EI_DF_{}_{}_{}.pkl').format(j, k, r)))

    comm_sub_pairs = maximalist_df[['Source_Level','Subreddit','Count']].groupby(['Source_Level','Subreddit']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_sub_pairs.to_pickle((id_l + date + ('/CSP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CSPPED')

    communities_series
    num_subreddits = comm_sub_pairs.groupby(level=0, axis=0).apply(lambda x : len(x))
    communities_df = pd.DataFrame({'Unique_Authors': communities_series, 'Unique_Subreddits': num_subreddits})
    communities_df.to_pickle(id_l + date + ('/COMMUNITIES_DF_{}_{}_{}.pkl').format(j, k, r))

    comm_auth_pairs= maximalist_df[['Source_Level','Source','Count']].groupby(['Source_Level','Source']).sum().groupby(level=0, axis=0).apply(lambda x : x.sort_values(by='Count', ascending=False)).droplevel(0)
    comm_auth_pairs.to_pickle((id_l + date + ('/CAP_{}_{}_{}.pkl').format(j, k, r)))
    print(r, date, 'CAPPED')


    top_c_dict = ei_df.sort_values(by=['Total_Links'], ascending=False).head(75)

    sub_sets = {}
    csp = comm_sub_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(70)).droplevel(0)
    for community in top_c_dict.index:
        sub_sets[community] = set(csp.loc[community].index)

    with open((id_l + date + ('/SUB_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ssh:
        pickle.dump(sub_sets, ssh)
        print(r, date, 'SUBSETT')

    auth_sets = {}
    cap = comm_auth_pairs.loc[top_c_dict.index].groupby(level=0, axis=0).apply(lambda x : x.head(1500)).droplevel(0)
    for community in top_c_dict.index:
        auth_sets[community] = set(cap.loc[community].index)

    with open((id_l + date + ('/AUTH_SETS_{}_{}_{}.pkl').format(j, k, r)), 'wb') as ash:
        pickle.dump(sub_sets, ash)
        print(r, date, 'AUTHSETT')
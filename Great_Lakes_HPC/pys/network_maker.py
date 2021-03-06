#!/sw/arcts/centos7/python3.8-anaconda/2021.05/bin/python
 
# Load  Python  modules #
#|||||||||||||||||||||||#

#import pkg_resources
#pkg_resources.require("decorator==5.1.0")
import os
import pandas as pd
#pkg_resources.require("pandas==1.3.5")
import networkx as nx
#pkg_resources.require("networkx==2.6.3")
from networkx.algorithms.link_analysis.pagerank_alg import pagerank

print('initialization of python script \n\n')

# quarters = pd.read_csv('/scratch/mmani_root/mmani0/shared_data/pushshift_python/Resources/Data/yearly_quarters.csv')
# print('quarters read\n')

weeks = pd.read_csv('/scratch/mmani_root/mmani0/shared_data/pushshift_python/Resources/Data/yearly_weeks.csv')
print('weeks read\n')

os.chdir('/scratch/mmani_root/mmani0/shared_data/hot/csvz/')
# os.chdir('/scratch/mmani_root/mmani0/shared_data/hot/csv_test/')

cols = ['post_type', 'subreddit', 'id', 'parent_id', 'link_id', 'url',
       'permalink', 'created_utc', 'datetime', 'score', 'upvote_ratio',
       'num_comments', 'controversiality', 'total_awards_received', 'stickied',
       'post_hint', 'is_self', 'is_video', 'title', 'body', 'author',
       'author_premium']

print('trying\n\n')
for file in os.listdir():
    if file.endswith('.csv'):
        print('\nparsing: '+file+'\n')
        try:
            comm = pd.read_csv(filepath_or_buffer=file, low_memory=False,)
            comm = comm[cols]
            # for i in range(len(quarters)):
            for i in range(len(weeks)):
                print('\nquarters loop :', i)
                if os.path.isfile(('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + weeks.iloc[i,6] + '/network_features_' + file)):
                    print('\n previously parsed at loop: {} \n'.format(i))
                    continue
                else:
                    try:
                        df = comm.copy()
                        
                        # lower_utc = df['created_utc'].astype('int64') >= quarters.iloc[i,3].astype('int64')
                        # upper_utc = df['created_utc'].astype('int64') <= quarters.iloc[i,5].astype('int64')
                        lower_utc = df['created_utc'].astype('int64') >= weeks.iloc[i,3].astype('int64')
                        upper_utc = df['created_utc'].astype('int64') <= weeks.iloc[i,5].astype('int64')
                        df = df[lower_utc & upper_utc]
                        
                        if len(df) <= 5:
                            print('loop skipped, file too small')
                            continue 

                        Authors = list()
                        Posts = list()
                        Post_Author_Pairs = dict()

                        for row in df.values:
                            Author = str(row[-2])
                            PostID = str(row[2])
                            
                            if Author != '[deleted]':
                                if Author != 'AutoModerator':
                                    Authors.append(Author)
                                    Posts.append(PostID)
                                    Post_Author_Pairs[PostID] = Author
                        try:
                            print('phase=1')

                            Data = {}

                            for row in df.values:
                                Author = str(row[-2])
                                PostType = str(row[0])
                                PostID = str(row[2])
                                LinkID = str(row[4])[3:]
                                ParentID = str(row[3])[3:]
                                
                                if Author != '[deleted]':
                                    if Author != 'AutoModerator':
                                        if LinkID in Post_Author_Pairs:
                                            ParentAuthor = Post_Author_Pairs[LinkID]
                                        elif ParentID in Post_Author_Pairs:
                                            ParentAuthor = Post_Author_Pairs[ParentID]
                                        else:
                                            ParentAuthor = ''

                                        if Author not in Data:
                                            Data[Author] = list()
                                    
                                        Data[Author].append([PostType, PostID, LinkID, ParentAuthor])
                        except Exception as e:
                            print('phase 1 failed as :', e)
                            continue

                        try:
                            print('phase=2')

                            Author_Exchanges = dict()

                            for Author, PostInfo in Data.items():
                                for Post in PostInfo:
                                    if Post[3] != '':
                                        Author_Exchanges[Author, Post[3]] = Author_Exchanges.get((Author, Post[3]), 0) + 1
                                    
                            No_Self_Exchanges = {}
                            ls = list(Author_Exchanges.keys())
                            j = 0

                            for author_pair, num_exchanges in Author_Exchanges.items():
                                if ls[j][0] != ls[j][1]:
                                    No_Self_Exchanges[author_pair] = num_exchanges
                                j += 1

                            G = nx.DiGraph()
                        except Exception as e:
                            print('phase 2 failed as :', e)
                            continue

                        try:
                            print('phase=3')

                            for Auth_Pair, Num_Exchanges in No_Self_Exchanges.items():
                                G.add_edge(Auth_Pair[0], Auth_Pair[1], weight=Num_Exchanges)

                            out_degrees = [G.out_degree(node) for node in G]
                            degree_centrality = nx.in_degree_centrality(G)
                            closeness_centrality = nx.closeness_centrality(G)
                            betweenness_centrality = nx.betweenness_centrality(G)
                            network_features = pd.DataFrame(
                                data={
                                    'in_degree': [(G.in_degree(node)) for node in G],
                                    'out_degree': out_degrees, 
                                    'degree_centrality': [degree_centrality[node] for node in G.nodes()],
                                    'closeness_centrality': [closeness_centrality[node] for node in G.nodes()],
                                    'betweenness_centrality': [betweenness_centrality[node] for node in G.nodes()]
                                    }, 
                                index=list(G.nodes())
                                )
                        except Exception as e:
                            print('phase 3 failed as :', e)
                            continue

                        try:
                            print('phase=4')

                            for aleph in [0.65,0.70,0.75,0.80,0.85,0.90,0.95]:
                                ranks = pagerank(G, alpha=aleph, max_iter=1000)
                                pr = [ranks[node] for node in G]
                                col_label = str(aleph)[2:] + '_pagerank'
                                network_features[col_label] = pr
                        except Exception as e:
                            print('page rank failed as :', e)
                            continue

                        print('file phase')

                        try:
                            print('start try')
                            # network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i,6] + '/network_features_' + file)
                            # nx.write_gpickle(G, ('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i,6] + '/network_G_' + file[:-4] + '.pkl'))
                            network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + weeks.iloc[i,6] + '/network_features_' + file)
                            nx.write_gpickle(G, ('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + weeks.iloc[i,6] + '/network_G_' + file[:-4] + '.pkl'))
                            print('files written successfully')
                        except:
                            print('start except')
                            # print('making dirs :', quarters.iloc[i,6])
                            # os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i,6] + '/')
                            # os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i,6] + '/')
                            # network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i,6] + '/network_features_' + file)
                            # nx.write_gpickle(G, ('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i,6] + '/network_G_' + file[:-4] + '.pkl'))
                            print('making dirs :', weeks.iloc[i,6])
                            os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + weeks.iloc[i,6] + '/')
                            os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + weeks.iloc[i,6] + '/')
                            network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + weeks.iloc[i,6] + '/network_features_' + file)
                            nx.write_gpickle(G, ('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + weeks.iloc[i,6] + '/network_G_' + file[:-4] + '.pkl'))
                            continue
                    except Exception as e:
                        print('quarters loop exception as :', e)
                        continue
                
        except Exception as e:
            print('file exception as :', e)
            continue     

print('\n\ntermination of python script \n\n')

exit()
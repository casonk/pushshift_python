#!/sw/arcts/centos7/python3.8-anaconda/2021.05/bin/python
 
# Load  Python  modules #
#|||||||||||||||||||||||#

import os
import pandas as pd
import networkx as nx
from networkx.algorithms.link_analysis.pagerank_alg import pagerank

os.chdir('/scratch/mmani_root/mmani0/shared_data/pushshift_python/Resources/Data/')
quarters = pd.read_csv('yearly_quarters.csv')
quarters['IDX'] = quarters['Year'].astype('str') + '_' + quarters['Quarter'].astype('str')

os.chdir('/scratch/mmani_root/mmani0/shared_data/hot/')
for file in os.listdir('csvz'):
    if file.endswith('.csv'):
        print(file)
        try:
            comm = pd.read_csv(filepath_or_buffer=file, low_memory=False,)

            for i in range(len(quarters)):
                df = comm.copy()
                
                lower_utc = df['utc'].astype('int64') >= quarters.iloc[i]['Start_Epoch'].astype('int64')
                upper_utc = df['utc'].astype('int64') <= quarters.iloc[i]['End_Epoch'].astype('int64')

                df = df[lower_utc & upper_utc]
                
                if len(df) <= 100:
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

                Data = {}

                for row in df.values:
                    Author = str(row[-2])
                    PostType = str(row[1])
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
                        
                Author_Exchanges = dict()

                for Author, PostInfo in Data.items():
                    for Post in PostInfo:
                        if Post[3] != '':
                            Author_Exchanges[Author, Post[3]] = Author_Exchanges.get((Author, Post[3]), 0) + 1
                        
                No_Self_Exchanges = {}
                ls = list(Author_Exchanges.keys())
                i = 0

                for author_pair, num_exchanges in Author_Exchanges.items():
                    if ls[i][0] != ls[i][1]:
                        No_Self_Exchanges[author_pair] = num_exchanges
                    i += 1

                G = nx.DiGraph()

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

                for aleph in [0.65,0.70,0.75,0.80,0.85,0.90,0.95]:
                    ranks = pagerank(G, alpha=aleph)
                    pr = [ranks[node] for node in G]
                    col_label = str(aleph)[2:] + '_pagerank'
                    network_features[col_label] = pr

                try:
                    network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i]['IDX'] + '/network_features_' + file)
                    nx.write_gpickle(G, '/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i]['IDX'] + '/network_G_' + file[:-3] + '.pkl')
                except:
                    print('making dir :', quarters.iloc[i]['IDX'])
                    os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i]['IDX'])
                    os.mkdir('/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i]['IDX'])
                    network_features.to_csv('/scratch/mmani_root/mmani0/shared_data/hot/csv_networkz/' + quarters.iloc[i]['IDX'] + '/network_features_' + file)
                    nx.write_gpickle(G, '/scratch/mmani_root/mmani0/shared_data/hot/pkl_networkz/' + quarters.iloc[i]['IDX'] + '/network_G_' + file[:-3] + '.pkl')
                        
        except Exception as e:
            print('exception as :', e)
            continue     

exit()
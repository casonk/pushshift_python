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

irrel = [
    'automoderator',        '[deleted]',            'HCE_Replacement_Bot',  'Rangers_Bot', 
    'dropbox_bot',          'Website_Mirror_Bot',   'Metric_System_Bot',    'Fedora-Tip-Bot',
    'some_bot',             'Brigade_Bot',          'Link_Correction_Bot',  'Porygon-Bot',
    'karmaconspiracy_Bot',  'SWTOR_Helper_Bot',     'annoying_yes_bot',     'Antiracism_Bot',
    'qznc_bot',             'mma_gif_bot',          'QUICHE-BOT',           'bRMT_Bot',
    'hockey_gif_bot',       'nba_gif_bot',          'gifster_bot',          'imirror_bot',
    'okc_rating_bot',       'tennis_gif_bot',       'nfl_gif_bot',          'CPTModBot',
    'LocationBot',          'CreepySmileBot',       'FriendSafariBot',      'WritingPromptsBot',
    'CreepierSmileBot',     'Cakeday-Bot',          'Meta_Bot',             'soccer_gif_bot',
    'gunners_gif_bot',      'xkcd_number_bot',      'PokemonFlairBot',      'ChristianityBot',
    'cRedditBot',           'StreetFightMirrorBot', 'FedoraTipAutoBot',     'UnobtaniumTipBot',
    'astro-bot',            'TipMoonBot',           'PlaylisterBot',        'Wiki_Bot',
    'fedora_tip_bot',       'GunnersGifsBot',       'PGN-Bot',              'GunnitBot',
    'havoc_bot',            'Relevant_News_Bot',    'gfy_bot',              'RealtechPostBot',
    'imgurHostBot',         'Gatherer_bot',         'JumpToBot',            'DeltaBot',
    'Nazeem_Bot',           'PhoenixBot',           'AtheismModBot',        'IsItDownBot',
    'RFootballBot',         'KSPortBot',            'CompileBot',           'SakuraiBot',
    'asmrspambot',          'SurveyOfRedditBot',    'rule_bot',             'xkcdcomic_bot',
    'PloungeMafiaVoteBot',  'PoliticBot',           'Dickish_Bot_Bot',      'SuchModBot',
    'MultiFunctionBot',     'CasualMetricBot',      'xkcd_bot',             'VerseBot',
    'BeetusBot',            'GameDealsBot',         'BadLinguisticsBot',    'rhiever-bot',
    'gfycat-bot-sucksdick', 'chromabot',            'Readdit_Bot',          'disapprovalbot',
    'request_bot',          'define_bot',           'dogetipbot',           'techobot',
    'CaptionBot',           'rightsbot',            'colorcodebot',         'roger_bot',
    'ADHDbot',              'hearing-aid_bot',      'WikipediaCitationBot', 'PonyTipBot',
    'fact_check_bot',       'rusetipbot',           'classybot',            'NFLVideoBot',
    'MAGNIFIER_BOT',        'WordCloudBot2',        'JotBot',               'WeeaBot',
    'raddit-bot',           'tipmoonbot2',          'haiku_robot',          'ttumblrbots',
    'givesafuckbot',        'gabentipbot',          'serendipitybot',       'autowikibot',
    'topredditbot',         'ddlbot',               'bitofnewsbot',         'conspirobot',
    'bot',                  'Definition_Bot',       'redditbots',           'autourbanbot',
    'randnumbot',           'VideoLinkBot',         'transcribot',          'vertpornpostbot',
    'vpbot14',              'verticalgifbot',       'animemod',             'nfl_mod',
    'groupbot',             'jobautomator',         'cricketmatchbot',      'ukpolbot',
    'politicsmoderatorbot', 'usi-bot',              'fplmoderator',         'commentorofposts',
    'nba_mod',              '2soccer2bot',          'ffbot',                'wishlistbot',
    'bodybuildingbot',      'latherbot',            'kickopenthedoorbot',   'fantasymod',
    'steroidsbot',          'darnbot',              'cfb_referee',          'cbbbot',
    'sbpotdbot',            'twitterinfo_bot',      'nfcaaofficialrefbot',  'rlcd-bot',
    'hw2-bot',              'hwsbot',               'feetpicsbot',          'nfl_gamethread',
    'cursedrobot',          'sexstatsbot',          'judgement_bot_aita',   'repostsleuthbot',
    'savevideo',            'mytoppost',            'transcribersofreddit', 'keepingdankmemesdank',
    'virtualautumn',        'muchmuchkarma',        'gdt_bot',              'nfl_gdt_bot',
    'rnews_mod',            'goodbotautomod',
    ]

irrel = [i.lower() for i in irrel]

j=1
k=21
keys = []
id_l = '/home/casonk/path/mmani_root/mmani0/shared_data/hot/push_file/IDL/'

for i in range(len(start_dates)):
    if os.path.isfile((id_l + date + ('/G_{}_{}.pkl').format(j, k))):
        continue
    date = _center_dates[i] 
    file = (id_l + date + ('/EDGE_LIST_SELFLESS_{}_{}.pkl'.format(j,k)))
    keys += [date]
    df = pd.read_pickle(file)
    source_mask = df['Source'].isin(irrel)
    target_mask = df['Target'].isin(irrel)

    trimmed_df = df[(~source_mask) & (~target_mask)]
    trimmed_df.to_pickle((id_l + date + ('/TRIMMED_DF_{}_{}.pkl').format(j, k)))

    G = nx.Graph()
    for info in trimmed_df.values:
        G.add_edge(info[0], info[1], subreddit=info[2], weight=int(info[3]))
    print(date, j, k)
    
    with open((id_l + date + ('/G_{}_{}.pkl').format(j, k)), 'wb') as Gh:
        pickle.dump(G, Gh)

r=0.5
for date in _center_dates:
    if os.path.isfile((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r))):
        continue
    with open((id_l + date + ('/G_{}_{}.pkl').format(j, k)), 'rb') as Gh:
        G = pickle.load(Gh)

    lc = community.louvain_communities(G, weight='weight', resolution=r, threshold=1e-07, seed=123)
    print(date, len(lc), r)

    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lch:
        pickle.dump(lc, lch)

r=1
for date in _center_dates:
    if os.path.isfile((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r))):
        continue
    with open((id_l + date + ('/G_{}_{}.pkl').format(j, k)), 'rb') as Gh:
        G = pickle.load(Gh)

    lc = community.louvain_communities(G, weight='weight', resolution=r, threshold=1e-07, seed=123)
    print(date, len(lc), r)

    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lch:
        pickle.dump(lc, lch)

r=2
for date in _center_dates:
    if os.path.isfile((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r))):
        continue
    with open((id_l + date + ('/G_{}_{}.pkl').format(j, k)), 'rb') as Gh:
        G = pickle.load(Gh)

    lc = community.louvain_communities(G, weight='weight', resolution=r, threshold=1e-07, seed=123)
    print(date, len(lc), r)

    with open((id_l + date + ('/LC_{}_{}_{}.pkl').format(j, k, r)), 'wb') as lch:
        pickle.dump(lc, lch)
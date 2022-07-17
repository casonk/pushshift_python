import pandas as pd 
import zstandard
from datetime import datetime
import json
import csv
import os

comm_file1 = 'R:\\Funded\\Ethical_Reccomendations\\Mass_Data\\Push_File\\RC\\2019+\\RC_2020-12.zst'
comm_file2 = 'R:\\Funded\\Ethical_Reccomendations\\Mass_Data\\Push_File\\RC\\2019+\\RC_2021-01.zst'
subm_file1 = 'R:\\Funded\\Ethical_Reccomendations\\Mass_Data\\Push_File\\RS\\2019+\\RS_2020-12.zst'
subm_file2 = 'R:\\Funded\\Ethical_Reccomendations\\Mass_Data\\Push_File\\RS\\2019+\\RS_2021-01.zst'

irrel = [
    'AutoModerator',        '[deleted]',            'HCE_Replacement_Bot',  'Rangers_Bot', 
    'DropBox_Bot',          'Website_Mirror_Bot',   'Metric_System_Bot',    'Fedora-Tip-Bot',
    'Some_Bot',             'Brigade_Bot',          'Link_Correction_Bot',  'Porygon-Bot',
    'KarmaConspiracy_Bot',  'SWTOR_Helper_Bot',     'annoying_yes_bot',     'Antiracism_Bot',
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
    'politicsmoderatorbot', 'usi-bot',              'fplmoderator'
    ]

irrel = [a.lower() for a in irrel]

_end_dates    = pd.Series(pd.date_range(start="2020-12-12", end="2021-01-31", freq="D", tz='America/New_York'))
_start_dates  = _end_dates - pd.Timedelta(days=7)
_center_dates = _end_dates - pd.Timedelta(days=3.5)
end_dates     = _end_dates.apply(lambda x: x.timestamp())
center_dates  = _center_dates.apply(lambda x: x.timestamp())
start_dates   = _start_dates.apply(lambda x: x.timestamp())
_end_dates    = _end_dates.apply(lambda x:str(x).split(' ')[0])
_center_dates = _center_dates.apply(lambda x:str(x).split(' ')[0])
_start_dates  = _start_dates.apply(lambda x:str(x).split(' ')[0])

def read_lines_zst(fh):
    with open(fh, "rb") as file_handle:
        buffer = ""
        reader = zstandard.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(
            file_handle
        )
        while True:
            chunk = reader.read(2 ** 27).decode()
            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]
        reader.close()

id_l = 'R:\\Funded\\Ethical_Reccomendations\\Mass_Data\\Push_File\\Unncomp\\v6\\'

errors = 0
lines = 0
skipped = 0

def create_common_data(post):
    """
    Helper function to collect values common between both comments and submissions.
    """

    try:
        subreddit = post["subreddit"]
        post_id = post["id"]
        try:
            link_id = post["link_id"]
        except KeyError:
            link_id = "nan"
        created_utc = post["created_utc"]
        author = post["author"]
        author = r"{}".format(author)

        post_data = {
            "author": author,
            "id": post_id,
            "link_id": link_id[3:],
            "subreddit": subreddit,
            "created_utc": created_utc,
        }
        return post_data
    except KeyboardInterrupt:
        pass
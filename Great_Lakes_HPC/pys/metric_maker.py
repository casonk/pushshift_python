import pandas as pd
import numpy as np
import os

conspiracy_topic = [
    '911truth', 
    'actualconspiracies', 
    'conspiracy_commons',
    'c_s_t',
    'conspiracy',
    'conspiracytheories',
    'conspiracyultra',
    'conspiracyundone',
    'culturallayer',
    'epstein',
    'flatearth',
    'globeskeptic',
    'mandelaeffect',
    'pedogate',
    'qult_headquarters',
    'ufos',
    'unresolvedmysteries',
    ]
political_topic = [
    'hankaaronaward',
    'joerogan',
    'nonewnormal',
    'the_donald',
    'watchredditdie',
    'wayofthebern', 
    ]
neutral_topic = [
    'datasets',
    'ford',
    'highstrangness',
    'honda',
    'linux',
    'math',
    'narcos',
    'outoftheloop',
    'pushshift',
    'snowfall',
    'subredditdrama',
    'undelete',
    'volvo',
    ]

print('Topics Defined \n')

source_dir = '/scratch/mmani_root/mmani0/shared_data/hot/csv_labelz/'
os.chdir(source_dir)

conspiracy_out_dir = '/scratch/mmani_root/mmani0/shared_data/hot/csv_ekoz/conspiracy/'
political_out_dir = '/scratch/mmani_root/mmani0/shared_data/hot/csv_ekoz/political/'
neutral_out_dir = '/scratch/mmani_root/mmani0/shared_data/hot/csv_ekoz/neutral/'

print('Globals Defined \n')

def conspiracy_labler(ref):
    ref = eval(ref)
    if len(ref) > 0:
        on_topic = 0
        off_topic = 0
        for r in ref:
            if r.lower() in conspiracy_topic:
                on_topic += 1
            else:
                off_topic += 1
        if on_topic >= off_topic:
            return 1
        else:
            return -1
    else:
        return 0

def political_labler(ref):
    ref = eval(ref)
    if len(ref) > 0:
        on_topic = 0
        off_topic = 0
        for r in ref:
            if r.lower() in political_topic:
                on_topic += 1
            else:
                off_topic += 1
        if on_topic >= off_topic:
            return 1
        else:
            return -1
    else:
        return 0
    
def neutral_labler(ref):
    ref = eval(ref)
    if len(ref) > 0:
        on_topic = 0
        off_topic = 0
        for r in ref:
            if r.lower() in neutral_topic:
                on_topic += 1
            else:
                off_topic += 1
        if on_topic >= off_topic:
            return 1
        else:
            return -1
    else:
        return 0

def meta_tricks(data, fname, topic_matter):
    label_data = data.copy(deep=True)
    label_data.set_index('id', inplace=True)
    label_data.drop(['subreddit', 'url_direct_ref', 'body_direct_ref', 'body_indirect_ref', 'title_indirect_ref'], axis=1, inplace=True)

    if topic_matter == 'conspiracy':
        label_data['label'] = label_data['refs'].apply(lambda x: conspiracy_labler(x))
        out_dir = conspiracy_out_dir
    elif topic_matter == 'political':
        label_data['label'] = label_data['refs'].apply(lambda x: political_labler(x))
        out_dir = political_out_dir
    elif topic_matter == 'neutral':
        label_data['label'] = label_data['refs'].apply(lambda x: neutral_labler(x))
        out_dir = neutral_out_dir

    dm = label_data['author'] == '[deleted]'
    label_data = label_data[~dm]
    zm = label_data['label'] == 0
    label_data = label_data[~zm]

    auths = label_data.groupby('author')
    summary = pd.DataFrame(auths['label'].value_counts()).unstack().fillna(0).droplevel(level=0, axis=1)
    try:
        summary.columns = ['off_topic', 'on_topic']
    except:
        if 1 in summary.columns:
            summary.columns = ['on_topic']
            summary['off_topic'] = 0
        elif -1 in summary.columns:
            summary.columns = ['off_topic']
            summary['on_topic'] = 0

    idxs = [i/20 for i in range(21,20001)]

    Is = []

    alpha_naives = []
    beta_naives = []
    gamma_naives = []
    seperation_naives = []
    isolation_naives = []
    echochamberness_naives = []

    alpha_totalities = []
    beta_totalities = []
    gamma_totalities = []
    seperation_totalities = []
    isolation_totalities = []
    echochamberness_totalities = []

    alpha_variances = []
    beta_variances = []
    gamma_variances = []
    seperation_variances = []
    isolation_variances = []
    echochamberness_variances = []

    alpha_logvariances = []
    beta_logvariances = []
    gamma_logvariances = []
    seperation_logvariances = []
    isolation_logvariances = []
    echochamberness_logvariances = []

    summary_copy = summary.copy(deep=True)
    for _lambda in idxs:
        summary = summary_copy.copy(deep=True)

        off_mask = summary['off_topic'] >= _lambda * summary['on_topic']
        on_mask = summary['on_topic'] >= _lambda * summary['off_topic']
        olappers = ~(off_mask|on_mask)

        summary.loc[off_mask,'off_topic'] = (summary.loc[off_mask,'off_topic'] - summary.loc[off_mask,'on_topic'])
        summary.loc[off_mask,'on_topic'] = 0
        summary.loc[on_mask,'on_topic'] = (summary.loc[on_mask,'on_topic'] - summary.loc[on_mask,'off_topic'])
        summary.loc[on_mask,'off_topic'] = 0

        alpha_frame = summary[on_mask]
        alpha_frame.drop('off_topic', axis=1, inplace=True)
        beta_frame = summary[off_mask]
        beta_frame.drop('on_topic', axis=1, inplace=True)
        gamma_frame = summary[olappers]
        gamma_frame['total'] = gamma_frame['off_topic'] + gamma_frame['on_topic']
        gamma_frame.drop(['off_topic','on_topic'], axis=1, inplace=True)

        I = int(alpha_frame.sum()) + int(beta_frame.sum()) + int(gamma_frame.sum())
        Is.append(I)

        alpha_naive = len(alpha_frame) / I
        beta_naive = len(beta_frame) / I
        gamma_naive = len(gamma_frame) / I
        alpha_naives += [alpha_naive]
        beta_naives += [beta_naive]
        gamma_naives += [gamma_naive]
        seperation_naives += [(4 * alpha_naive * beta_naive)]
        isolation_naives += [(alpha_naive / (alpha_naive + beta_naive))]
        echochamberness_naives += [(alpha_naive / (alpha_naive + beta_naive + gamma_naive))]

        alpha_totality = alpha_frame['on_topic'].sum() / I
        beta_totality = beta_frame['off_topic'].sum() / I
        gamma_totality = gamma_frame['total'].sum() / I
        alpha_totalities += [alpha_totality]
        beta_totalities += [beta_totality]
        gamma_totalities += [gamma_totality]
        seperation_totalities += [(4 * alpha_totality * beta_totality)]
        isolation_totalities += [(alpha_totality / (alpha_totality + beta_totality))]
        echochamberness_totalities += [(alpha_totality / (alpha_totality + beta_totality + gamma_totality))]

        alpha_variance = np.ceil(alpha_frame['on_topic'] / (1 + alpha_frame['on_topic'].var())).sum() / I
        beta_variance = np.ceil(beta_frame['off_topic'] / (1 + beta_frame['off_topic'].var())).sum() / I
        gamma_variance = np.ceil(gamma_frame['total'] / (1 + gamma_frame['total'].var())).sum() / I
        alpha_variances += [alpha_variance]
        beta_variances += [beta_variance]
        gamma_variances += [gamma_variance]
        seperation_variances += [(4 * alpha_variance * beta_variance)]
        isolation_variances += [(alpha_variance / (alpha_variance + beta_variance))]
        echochamberness_variances += [(alpha_variance / (alpha_variance + beta_variance + gamma_variance))]

        alpha_logvariance = np.ceil(alpha_frame['on_topic'] / (1 + np.log(1 + alpha_frame['on_topic'].var()))).sum() / I
        beta_logvariance = np.ceil(beta_frame['off_topic'] / (1 + np.log(1 + beta_frame['off_topic'].var()))).sum() / I
        gamma_logvariance = np.ceil(gamma_frame['total'] / (1 + np.log(1 + gamma_frame['total'].var()))).sum() / I
        alpha_logvariances += [alpha_logvariance]
        beta_logvariances += [beta_logvariance]
        gamma_logvariances += [gamma_logvariance]
        seperation_logvariances += [(4 * alpha_logvariance * beta_logvariance)]
        isolation_logvariances += [(alpha_logvariance / (alpha_logvariance + beta_logvariance))]
        echochamberness_logvariances += [(alpha_logvariance / (alpha_logvariance + beta_logvariance + gamma_logvariance))]

    df = pd.DataFrame({
            'idx':idxs, 
            'I':Is,
            'alpha_naive':alpha_naives,
            'beta_naive':beta_naives,
            'gamma_naive':gamma_naives,
            'seperation_naive':seperation_naives,
            'isolation_naive':isolation_naives,
            'echochamberness_naive':echochamberness_naives,
            'alpha_totality':alpha_totalities,
            'beta_totality':beta_totalities,
            'gamma_totality':gamma_totalities,
            'seperation_totality':seperation_totalities,
            'isolation_totality':isolation_totalities,
            'echochamberness_totality':echochamberness_totalities,
            'alpha_variance':alpha_variances,
            'beta_variance':beta_variances,
            'gamma_variance':gamma_variances,
            'seperation_variance':seperation_variances,
            'isolation_variance':isolation_variances,
            'echochamberness_variance':echochamberness_variances,
            'alpha_logvariance':alpha_logvariances,
            'beta_logvariance':beta_logvariances,
            'gamma_logvariance':gamma_logvariances,
            'seperation_logvariance':seperation_logvariances,
            'isolation_logvariance':isolation_logvariances,
            'echochamberness_logvariance':echochamberness_logvariances
            })

    df.set_index('idx', inplace=True)
    df.to_csv((out_dir + 'eko_' + fname))

    print(fname, 'completed for', topic_matter, '\n')
    

print('Functions Defined \n')

for file in os.listdir():
    fname = file[6:]
    print('\n' + fname + '\n')

    data = pd.read_csv(file, low_memory=False)
    meta_tricks(data, fname, 'conspiracy')
    meta_tricks(data, fname, 'political')
    meta_tricks(data, fname, 'neutral')
    
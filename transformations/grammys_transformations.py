import pandas as pd
import re



def replacing_nulls_cond_1(df_grammys):
    condition_1 = df_grammys['artist'].isnull() & df_grammys['workers'].str.contains(r'\(.*\)')
    df_grammys.loc[condition_1, 'artist'] = df_grammys.loc[condition_1, 'workers'].apply(lambda x: re.search(r'\((.*?)\)', x).group(1) if isinstance(x, str) and re.search(r'\((.*?)\)', x) else None)  
    return df_grammys

def replacing_nulls_cond_2(df_grammys):
    condition_2= df_grammys['workers'].str.contains('[;,]', na=False) & ~df_grammys['workers'].str.contains(r'\(.*\)', na=False) & df_grammys['artist'].isnull()
    df_grammys.loc[condition_2, 'artist'] = df_grammys.loc[condition_2, 'workers'].str.split('[;,]').str[0].str.strip()
    return df_grammys

def replacing_nulls_cond_3(df_grammys):
    condition_3 = df_grammys['category'].str.contains('Best New Artist') | df_grammys['category'].str.contains('Best New Artist Of') & df_grammys['artist'].isnull()
    df_grammys.loc[condition_3, 'artist'] = df_grammys.loc[condition_3, 'nominee']
    return df_grammys

def replacing_nulls_cond_4(df_grammys):
    condition_4 = df_grammys['category'].str.contains('Producer Of The Year') | df_grammys['category'].str.contains ('Producer Of The Year Non-Classical')
    df_grammys.loc[condition_4, 'artist'] = df_grammys.loc[condition_4, 'nominee']
    return df_grammys

def replacing_nulls_cond_5(df_grammys):  
    condition_5 = df_grammys['artist'].isnull() & (df_grammys['category'].str.contains('Gospel Performance') | df_grammys['category'].str.contains('Small Ensemble Performance') | df_grammys['category'].str.contains('Music Performance'))
    df_grammys.loc[condition_5, 'artist'] = df_grammys.loc[condition_5, 'workers']
    return df_grammys

def replacing_nulls_cond_6(df_grammys):
    condition_6 = df_grammys['artist'].isnull() & df_grammys['category'].str.contains('Solo')
    df_grammys.loc[condition_6, 'artist'] = df_grammys.loc[condition_6, 'workers']

    return df_grammys

def replacing_nulls_cond_7(df_grammys):
    condition_7 = df_grammys['artist'].isnull() & df_grammys['category'].str.contains('Producer')
    df_grammys.loc[condition_7, 'artist'] = df_grammys.loc[condition_7, 'nominee']

    return df_grammys

def removing_nulls(df_grammys):
    df_grammys = df_grammys.dropna()
    return df_grammys

def removing_unnecessary_columns(df_grammys):
    df_grammys = df_grammys.drop(columns=['workers', 'img', 'published_at', 'updated_at'])
    return df_grammys
def rename_columns(df_grammys):
    df_grammys.rename(columns={'winner': 'was_nominated', 'category': 'award'}, inplace=True)
    return df_grammys
def was_nominated_mapping(df_grammys):
    df_grammys['was_nominated'] = df_grammys['was_nominated'].map({True: 1, False: 0})
    return df_grammys
import pandas as pd

def fill_null_values_was_nomineed(df_merged):
    df_merged['was_nominated'] = df_merged['was_nominated'].fillna(0)
    return df_merged


def fill_null_values_year(df_merged):
    df_merged['year'] = df_merged['year'].fillna(-1)
    return df_merged

def fill_null_values_nominee(df_merged):
    df_merged['nominee'] = df_merged['nominee'].fillna('Not nominated')
    return df_merged

def fill_null_values_remaining(df_merged):
    df_merged['title'] = df_merged['title'].fillna('Not Applicable')
    df_merged['award'] = df_merged['award'].fillna('Not Applicable')
    return df_merged

def drop_artist_column(df_merged):
    df_merged.drop(['artist'], axis=1, inplace=True)
    return df_merged



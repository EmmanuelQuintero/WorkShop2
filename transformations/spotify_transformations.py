import pandas as pd

def drop_unnamed_0_column(df):

    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    return df
    

def drop_duplicates(df):
    df.drop_duplicates(inplace=True)
    return df

def drop_missing_values(df):
        index_to_drop = df[df['track_id'] == '1kR4gIb7nGxHPI3D2ifs59'].index
        df = df.drop(index_to_drop)
        return df


def create_popularity_level_column(df):
    df['popularity_level'] = df['popularity'].apply(lambda x: 'Baja' if x <= 40 else 'Media' if x < 70 else 'Alta')
    return df

def create_duration_mins_secs_column(df):
    df['duration_mins_secs'] = df['duration_ms'].apply(lambda x: f'{int(x/60000)}:{int((x/1000)%60):02d}')
    return df


def create_is_explicit_column(df):
    df['is_explicit'] = df['explicit'].apply(lambda x: 1 if x == True else 0)
    return df

def create_is_danceable_column(df):
    df['is_danceable'] = df['danceability'].apply(lambda x: 0 if x < 0.5 else 1)
    return df

def create_track_feeling_column(df):
    df['track_feeling'] = df['valence'].apply(lambda x: 'Positive' if x >= 0.5 else 'Negative')
    return df

def create_general_category_column(df):
    genre_to_category = {
    'acoustic': 'Instrumental', 'afrobeat': 'World Music', 'alt-rock': 'Rock', 'alternative': 'Alternative', 'ambient': 'Instrumental',
    'anime': 'Others', 'black-metal': 'Metal', 'bluegrass': 'Country', 'blues': 'Blues',
    'brazil': 'World Music', 'breakbeat': 'Electronic', 'british': 'World Music', 'cantopop': 'World Music', 'chicago-house': 'Electronic',
    'children': 'Others', 'chill': 'Instrumental', 'classical': 'Instrumental', 'club': 'Electronic', 'comedy': 'Others',
    'country': 'Country', 'dance': 'Electronic', 'dancehall': 'Reggae', 'death-metal': 'Metal', 'deep-house': 'Electronic',
    'detroit-techno': 'Electronic', 'disco': 'Electronic', 'disney': 'Others', 'drum-and-bass': 'Electronic',
    'dub': 'Electronic', 'dubstep': 'Electronic', 'edm': 'Electronic', 'electro': 'Electronic', 'electronic': 'Electronic',
    'emo': 'Rock', 'folk': 'Folk', 'forro': 'World Music', 'french': 'World Music', 'funk': 'Funk', 'garage': 'Rock', 'german': 'World Music',
    'gospel': 'Others', 'goth': 'Rock', 'grindcore': 'Punk', 'groove': 'Jazz', 'grunge': 'Rock', 'guitar': 'Instrumental',
    'happy': 'Others', 'hard-rock': 'Rock', 'hardcore': 'Punk', 'hardstyle': 'Electronic', 'heavy-metal': 'Metal', 'hip-hop': 'Hip-Hop',
    'honky-tonk': 'Country', 'house' : 'Electronic', 'idm': 'Electronic', 'indian': 'World Music', 'indie-pop' : 'Pop', 'indie': 'Rock',
    'industrial': 'Electronic', 'iranian': 'World Music', 'j-dance': 'Electronic', 'j-idol': 'Pop', 'j-pop': 'Pop', 'j-rock': 'Rock',
    'jazz': 'Jazz', 'k-pop': 'Pop', 'kids': 'Others', 'latin': 'World Music', 'latino': 'World Music','malay': 'World Music',
    'mandopop' : 'World Music', 'metal': 'Metal', 'metalcore': 'Metal', 'minimal-techno': 'Electronic','mpb': 'World Music',
    'new-age': 'Electronic', 'opera' : 'Vocal', 'pagode' : 'World Music', 'party': 'Others', 'piano': 'Instrumental',
    'pop-film': 'Others', 'pop': 'Pop', 'power-pop': 'Pop', 'progressive-house': 'Electronic', 'psych-rock': 'Rock', 'punk-rock': 'Punk',
    'punk': 'Punk', 'r-n-b': 'Blues', 'reggae': 'Reggae', 'reggaeton': 'Reggaeton', 'rock-n-roll': 'Rock', 'rock': 'Rock',
    'rockabilly': 'Rock', 'romance': 'Others', 'sad' : 'Others', 'salsa': 'Salsa', 'samba': 'World Music', 'sertanejo': 'World Music',
    'show-tunes': 'Others', 'singer-songwriter': 'Others', 'ska': 'World Music', 'sleep': 'Others', 'songwriter': 'Others',
    'soul': 'Blues', 'spanish': 'World Music', 'study': 'Others', 'swedish': 'World Music', 'synth-pop': 'Pop', 'tango': 'World Music',
    'techno': 'Electronic', 'trance': 'Electronic', 'trip-hop': 'Electronic', 'turkish': 'World Music','world-music': 'World Music'
    }
    df['general_category'] = df['track_genre'].map(genre_to_category)
    return df

def drop_columns(df):
    df.drop(['mode', 'valence', 'danceability','explicit', 'popularity', 'key', 'duration_ms', 'tempo', 'instrumentalness', 'time_signature', 'liveness', 'speechiness','track_genre', 'acousticness' ], axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df
    
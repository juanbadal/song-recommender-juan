
import numpy as np
import pandas as pd
import sys
from config import *
import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials
import time



def search_song(title: str, artist: str, limit: int = 5):
    
    '''
    Searches a song in the spotify API based on three parameters and returns the id
    Inputs:
    - title: name of song
    - artist: artist of song
    - limit
    Outputs:
    - results: id
    '''
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))
    
    search = title + ' ' + artist
    
    response = sp.search(q=search, limit=limit)
    
    results = response['tracks']['items'][0]['id']
    
    return results



def search_bulk(df: pd.DataFrame, col: str, limit: int=1) -> pd.DataFrame:
    
    '''
    Takes a dataframe and the name of a column, searches the song names in the spotify API and returns a dataframe of the song id, name and artist.
    Inputs:
    - df: DataFrame
    - col: column name
    - limit: default=1
    Outputs:
    - df2: DataFrame with ids, names and artists
    '''
    
    song_ids = []
    song_names = []
    song_artists = []
    
    counter = 0
    
    song_list = df[col].tolist()
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))
    
    for i in range(len(song_list)):
        try:
            results = sp.search(song_list[i], limit=limit)
            song_ids.append(results['tracks']['items'][0]['id'])
            song_names.append(results['tracks']['items'][0]['name'])
            song_artists.append(results['tracks']['items'][0]['artists'][0]['name'])
        except:
            print('Song not found!')
        
        print(f'{i}/{len(song_list)}')
        
        counter += 1
        
        if counter % 10 == 0:
            time.sleep(1)
            
    df2 = pd.DataFrame({'song_ids': song_ids, 'song_names': song_names, 'song_artists': song_artists})
    
    return df2



def get_audio_features(df: pd.DataFrame, id_col: str, limit: int=1, chunk_size: int=50) -> pd.DataFrame:
    
    '''
    Takes a list of song ids and outputs a dataframe with their audio features.
    '''
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))
    
    result_df = pd.DataFrame(columns=['id'])
    
    for i in range(0, len(df), chunk_size):
        try:
            chunk_ids = df[id_col].iloc[i:i+chunk_size].tolist()
            chunk_audio_features = sp.audio_features(chunk_ids)
            result_df = pd.concat([result_df, pd.DataFrame(chunk_audio_features)])
        except:
            print('Error processing songs!')
        
        print('Sleeping 5 seconds...')
        time.sleep(5)
        
    result_df = pd.merge(df, result_df, left_on=id_col, right_on='id')
    
    result_df.drop(columns='id', inplace=True)
    
    return result_df

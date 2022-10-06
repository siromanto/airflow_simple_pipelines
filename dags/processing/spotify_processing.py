import pprint
import json
from datetime import datetime, timedelta
import time
import requests
from processing.token_refresh import get_refreshed_token

# check if the airflow uses the correct python version

# export SPOTIPY_CLIENT_ID='c67895d91fb448f99b60ff4651092f7a'
# export SPOTIPY_CLIENT_SECRET='12dff0a738a4424390a28ad95a3d173f'


class Spotify:
    def __init__(self, relative_date_start, number_of_tracks=50) -> None:
        self.recently_played_tracks = self.get_recently_played_tracks(relative_date_start, number_of_tracks)
        self.audio_features = self.get_audio_features()


    def time_to_unix(self, date):
        if type(date) == str:
            date_obj = datetime.strptime(date, '%d/%m/%Y %H:%M:%S')
        else:
            date_obj = date
        unix_timestamp = datetime.timestamp(date_obj) * 1000
        return int(unix_timestamp)

    def get_recently_played_tracks(self, relative_date_start, number_of_tracks=50):
        """
        Check if the data which was parsed is enough - when we have more than 50 songs in a given date range
        Parse every hour for the updates of the listening history
        """

        self.track_history_info = []
        self.tracks_ids_unique = []
        relative_date_start = self.time_to_unix(relative_date_start)

        headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {get_refreshed_token()}"
        }

        track_history = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={relative_date_start}&limit={number_of_tracks}", headers=headers).json()
        for i in track_history["items"]:

            track_id = i["track"]["id"]
            played_at = i["played_at"]
            track_name = i["track"]["name"]
            track_artists = ", ".join([j["name"] for j in i["track"]["artists"]])
            track_album = i["track"]["album"]["name"]
            track_popularity = i["track"]["popularity"]


            self.track_history_info.append((track_id, played_at, track_name, track_artists, track_album, track_popularity))

            if track_id not in self.tracks_ids_unique:
                self.tracks_ids_unique.append(track_id)
            else:
                continue

        return self.track_history_info

            # if track_id in history_db:
                # get track features from there
            # else:
            #   retrieve the features from the spotify API
            
    def get_audio_features(self):
        """
        Method for retrieving the audio features of the track
        
        """

        self.audio_features_info = []

        headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {get_refreshed_token()}"
        }


        for item in self.tracks_ids_unique:
            audio_features = requests.get(f"https://api.spotify.com/v1/audio-features/{item}", headers=headers).json()

            danceability = audio_features["danceability"]
            energy = audio_features["energy"]
            key = audio_features["key"]
            loudness = audio_features["loudness"]
            mode = audio_features["mode"]
            speechiness = audio_features["speechiness"]
            acousticness = audio_features["acousticness"]
            instrumentalness = audio_features["instrumentalness"]
            liveness = audio_features["liveness"]
            valence = audio_features["valence"]
            tempo = audio_features["tempo"]
            duration_ms = audio_features["duration_ms"]
            time_signature = audio_features["time_signature"]

            self.audio_features_info.append((item, danceability, energy, key, 
                                        loudness, mode, speechiness, acousticness,
                                        instrumentalness, liveness, valence,
                                        tempo, duration_ms, time_signature))



        return self.audio_features_info


if __name__ == "__main__":
    current_date = datetime.now() - timedelta(hours=1)
    print(f"current date: {current_date}")
    spotify_obj = Spotify(current_date)
    print(spotify_obj.recently_played_tracks)
    # print()
    # print(spotify_obj.get_recently_played_tracks(current_date))
import requests
import datetime
import json

def get_refreshed_token():

    credentials = {
        "access_token":"",
        "token_type":"Bearer",
        "expires_in":3600,
        "refresh_token":"",
        "scope":"user-read-recently-played"
    }

    # 64-bit encoded client_id:client_secret
    headers = {
        "Authorization": 
        "Basic ="
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token" : ""
    }

    url = "https://accounts.spotify.com/api/token"

    request = requests.post(url, headers=headers, data=data)
    TOKEN = request.json()["access_token"]
    # TODO add if statement for requests status
    return TOKEN
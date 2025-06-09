from utils.file_utils import save_to_file

import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("LASTFM_API_KEY")
BASE_URL = "http://ws.audioscrobbler.com/2.0/"

def fetch_top_tracks(limit=500):
    params = {
        "method": "chart.gettoptracks",
        "api_key": API_KEY,
        "format": "json",
        "limit": limit
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    tracks = response.json().get("tracks", {}).get("track", [])

    save_to_file(tracks,"raw","lastfm","top_tracks","top_tracks.json",True)

def fetch_top_artists(limit=50):
    params = {
        "method": "chart.gettopartists",
        "api_key": API_KEY,
        "format": "json",
        "limit": limit
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    artists = response.json().get("artists", {}).get("artist", [])

    save_to_file(artists, "raw", "lastfm", "top_artists", "top_artists.json", True)

if __name__ == "__main__":
    fetch_top_tracks()
    fetch_top_artists()

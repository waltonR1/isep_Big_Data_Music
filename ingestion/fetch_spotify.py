from utils.file_utils import save_to_file

import requests
import os
import dotenv

dotenv.load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_access_token():
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]


def fetch_categories(locale="en-US"):
    access_token = get_access_token()

    category_url = f"https://api.spotify.com/v1/browse/categories?locale={locale}&limit=50"
    headers = {"Authorization": f"Bearer {access_token}"}
    cat_resp = requests.get(category_url, headers=headers)
    cat_resp.raise_for_status()
    categoriesList = cat_resp.json()["categories"]["items"]

    categories = [
        {"id": category["id"], "name": category["name"]}
        for category in categoriesList
    ]

    save_to_file(categories,"raw","spotify","categories","categories.json")


def fetch_playlists():
    access_token = get_access_token()

    playlist_url = f"https://api.spotify.com/v1/search?q=top%2550&type=playlist&limit=11"
    headers = {"Authorization": f"Bearer {access_token}"}
    playlist_resp = requests.get(playlist_url, headers=headers)
    playlist_resp.raise_for_status()
    playlist = playlist_resp.json()

    save_to_file(playlist,"raw","spotify","playlists","playlist.json")

def fetch_tracks():
    access_token = get_access_token()

    playlist_url = f"https://api.spotify.com/v1/search?q=top%2550&type=track&limit=1"
    headers = {"Authorization": f"Bearer {access_token}"}
    track_resp = requests.get(playlist_url, headers=headers)
    track_resp.raise_for_status()
    trackList = track_resp.json()

    save_to_file(trackList,"raw","spotify","tracklists","tracklist.json")

if __name__ == "__main__":
    # fetch_categories("zh_CN")
    fetch_playlists()
    fetch_tracks()

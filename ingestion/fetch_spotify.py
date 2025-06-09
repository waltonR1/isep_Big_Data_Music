from utils.file_utils import save_to_file

import requests
import time
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

def fetch_tracks():
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    query = "top%2050"
    limit = 50

    initial_url = f"https://api.spotify.com/v1/search?q={query}&type=track&limit=1"
    initial_resp = requests.get(initial_url, headers=headers)
    initial_resp.raise_for_status()
    initial_data = initial_resp.json()
    total = initial_data.get("tracks", {}).get("total", 0)
    print(f"Total tracks to fetch: {total}")

    all_tracks = []


    for offset in range(0, total, limit):
        url = f"https://api.spotify.com/v1/search?q={query}&type=track&limit={limit}&offset={offset}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page = response.json()
        items = page.get("tracks", {}).get("items", [])
        all_tracks.extend(items)
        print(f"Fetched {len(items)} tracks from offset {offset}")
        time.sleep(0.3)

    save_to_file(all_tracks,"raw","spotify","trackLists","trackLists.json",True)

if __name__ == "__main__":
    fetch_tracks()

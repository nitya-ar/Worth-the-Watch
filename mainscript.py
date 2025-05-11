import pandas as pd
import requests
import time
from justwatch import JustWatch

# === CONFIGURATION ===
TMDB_API_KEY = "d58935a3624cb12af6521bef5d335d5f"  # Replace this
# Example for Netflix:
INPUT_FILE = "netflix_catalog.csv"
OUTPUT_FILE = "netflix_enriched.csv"
PLATFORM_NAME = "Netflix"

# === TMDb API Endpoints ===
FIND_URL = "https://api.themoviedb.org/3/find/{}?api_key={}&external_source=imdb_id"
MOVIE_URL = "https://api.themoviedb.org/3/movie/{}?api_key={}"
TV_URL = "https://api.themoviedb.org/3/tv/{}?api_key={}"

# === Functions ===
def check_is_original(imdb_id, content_type, platform):
    try:
        find_resp = requests.get(FIND_URL.format(imdb_id, TMDB_API_KEY)).json()
        results = find_resp['movie_results'] if content_type == 'movie' else find_resp['tv_results']
        if not results:
            return False
        tmdb_id = results[0]['id']
        detail_url = MOVIE_URL if content_type == 'movie' else TV_URL
        meta = requests.get(detail_url.format(tmdb_id, TMDB_API_KEY)).json()
        companies = [c['name'].lower() for c in meta.get("production_companies", [])]
        return any(platform.lower() in company for company in companies)
    except:
        return None

def get_available_countries(title):
    try:
        justwatch = JustWatch(country='US')  # starting from US search
        results = justwatch.search_for_item(query=title)
        if 'items' in results and results['items']:
            offers = results['items'][0].get('offers', [])
            countries = list(set([offer['country'] for offer in offers if 'country' in offer]))
            return ','.join(countries)
        else:
            return None
    except:
        return None

# === Load & Prepare Data ===
df = pd.read_csv(INPUT_FILE)
df['platform'] = PLATFORM_NAME
df['is_original'] = None
df['available_countries'] = None

# === Enrichment Loop ===
for idx, row in df.iterrows():
    imdb_id = row['imdbId']
    title = row['title']
    content_type = row['type']
    df.at[idx, 'is_original'] = check_is_original(imdb_id, content_type, PLATFORM_NAME)
    df.at[idx, 'available_countries'] = get_available_countries(title)
    time.sleep(0.25)  # Respect rate limits

# === Save Output ===
df.to_csv(OUTPUT_FILE, index=False)
print(f"Done. Enriched file saved to: {OUTPUT_FILE}")

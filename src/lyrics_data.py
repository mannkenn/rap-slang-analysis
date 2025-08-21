import os
from dotenv import load_dotenv
import pandas as pd
from lyricsgenius import Genius
import time
import requests

# Load env variables
load_dotenv()
GENIUS_API_TOKEN = os.getenv("GENIUS_API_TOKEN")

# Create Genius client with delay and retries
genius = Genius(
    GENIUS_API_TOKEN,
    skip_non_songs=True,
    excluded_terms=["(Remix)", "(Live)", "(Demo)", "(The Tonight Show)"],
    remove_section_headers=True,
    retries=3
)

SAVE_EVERY = 5  # Save after every 5 rappers
SAVE_PATH = "../data/raw/all_lyrics2.csv"   

def get_lyrics_by_rapper(rapper_name, max_songs=300):
    lyrics_data = []
    
    try:
        artist = genius.search_artist(rapper_name, max_songs=max_songs, sort="popularity")
        if artist is None:
            print(f"Artist '{rapper_name}' not found.")
            return lyrics_data

        for song in artist.songs:
            try:
                song_data = song._body  # Raw Genius JSON data
                release_date = song_data.get("release_date", "N/A")

                lyrics_data.append({
                    'track_name': song.full_title,
                    'artist': song.artist,
                    'lyrics': song.lyrics,
                    'release_date': release_date
                })
            except Exception as e:
                print(f"Error processing song: {e}")
                continue

    # Rate limit
    except requests.exceptions.RequestException as e:
        print(f"Network/Rate error for {rapper_name}: {e}")
        print("Sleeping for 60 seconds before retry...")
        time.sleep(60)
    # Other errors
    except Exception as e:
        print(f"Error fetching data for {rapper_name}: {e}")

    return lyrics_data

if __name__ == "__main__":
    rapper_names = pd.read_csv('../data/raw/rapper_names.csv', index_col=0)
    
    # If continuing from saved progress
    if os.path.exists(SAVE_PATH):
        existing_df = pd.read_csv(SAVE_PATH)
        processed_rappers = set(existing_df['artist'].unique())
        print(f"Resuming from previous run. {len(processed_rappers)} artists already processed.")
    else:
        existing_df = pd.DataFrame()
        processed_rappers = set()

    all_lyrics = []

    for idx, rapper in enumerate(rapper_names['Name'].unique()):
        if rapper in processed_rappers:
            print(f"Skipping already processed: {rapper}")
            continue

        print(f"Getting lyrics for {rapper}")
        data = get_lyrics_by_rapper(rapper)

        if not data:
            print(f"No data found for {rapper}")
            continue

        all_lyrics.extend(data)

        # Save every SAVE_EVERY rappers
        if (idx + 1) % SAVE_EVERY == 0:
            temp_df = pd.DataFrame(all_lyrics)
            all_lyrics = []  # reset buffer
            combined = pd.concat([existing_df, temp_df], ignore_index=True)
            combined.to_csv(SAVE_PATH, index=False, encoding='utf-8')
            existing_df = combined  # update reference
            print(f"Saved progress after {idx + 1} rappers.\n")

        # Respectful pause between artists
        time.sleep(3)

    # Final save
    if all_lyrics:
        temp_df = pd.DataFrame(all_lyrics)
        combined = pd.concat([existing_df, temp_df], ignore_index=True)
        combined.to_csv(SAVE_PATH, index=False, encoding='utf-8')
        print("Final save complete.")
        



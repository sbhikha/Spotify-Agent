# data_collection/spotify_client.py

import os
import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from dotenv import load_dotenv

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv() # Load environment variables from .env file

# --- Environment Variable Check ---
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

if not all([SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI]):
    logging.error("Spotify API credentials not found in environment variables.")
    raise ValueError("Missing Spotify API credentials in .env file (SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI)")

# Define the scope of permissions needed
# Find available scopes here: https://developer.spotify.com/documentation/web-api/concepts/scopes
SCOPE = "user-library-read playlist-read-private user-read-recently-played"

class SpotifyClient:
    """
    A client to interact with the Spotify Web API using Spotipy.
    Handles authentication and provides methods to fetch user data.
    """
    def __init__(self, client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE):
        """
        Initializes the Spotify client and authenticates the user.
        """
        self.sp = None
        try:
            # SpotifyOAuth handles the token caching and refreshing automatically
            # It will cache the token in a file named .cache in your project directory
            # auth_manager = SpotifyOAuth(
            #     client_id=client_id,
            #     client_secret=client_secret,
            #     redirect_uri=redirect_uri,
            #     scope=scope,
            #     cache_path=".spotify_token_cache" # Explicitly name the cache file
            # )
            auth_manager = SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
            self.sp = spotipy.Spotify(auth_manager=auth_manager)
            # Try making a simple call to confirm authentication
            self.sp.current_user()
            logging.info("Successfully authenticated with Spotify.")
        except spotipy.SpotifyException as e:
            logging.error(f"Spotify authentication failed: {e}")
            # This might happen if the token is expired and refresh fails,
            # or if initial authentication requires user interaction (opening browser).
            # Running the script interactively the first time usually solves this.
            print("\n>>> Spotify Auth Error <<<\nPlease ensure your credentials in .env are correct and try running this script interactively once to authorize.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during Spotify client initialization: {e}")
            raise

    def get_saved_tracks(self, limit_per_req=50, max_tracks=None):
        """
        Retrieves the current user's saved tracks (Liked Songs).

        Args:
            limit_per_req (int): Number of tracks to fetch per API request (max 50).
            max_tracks (int, optional): Maximum total tracks to retrieve. Defaults to None (retrieve all).

        Returns:
            list: A list of dictionaries, where each dictionary contains
                  information about a saved track (id, name, artists, album, added_at).
                  Returns an empty list if an error occurs or no tracks are found.
        """
        if not self.sp:
            logging.error("Spotify client not initialized.")
            return []

        saved_tracks = []
        offset = 0
        total_fetched = 0

        logging.info(f"Fetching saved tracks from Spotify (limit_per_req={limit_per_req}, max_tracks={max_tracks or 'All'})...")

        while True:
            try:
                results = self.sp.current_user_saved_tracks(limit=limit_per_req, offset=offset)
                if not results or not results.get('items'):
                    logging.info("No more saved tracks found.")
                    break

                items = results['items']
                for item in items:
                    track_info = item.get('track')
                    if track_info:
                        artists = [{'id': artist['id'], 'name': artist['name']} for artist in track_info['artists']]
                        album = {'id': track_info['album']['id'], 'name': track_info['album']['name']}
                        saved_tracks.append({
                            'id': track_info['id'],
                            'name': track_info['name'],
                            'artists': artists,
                            'album': album,
                            'duration_ms': track_info['duration_ms'],
                            'popularity': track_info['popularity'],
                            'external_url': track_info['external_urls']['spotify'],
                            'added_at': item['added_at'] # Timestamp when the track was saved
                        })
                        total_fetched += 1

                logging.info(f"Fetched {len(items)} tracks. Total fetched so far: {total_fetched}")

                if results.get('next'):
                    offset += limit_per_req
                    if max_tracks is not None and total_fetched >= max_tracks:
                        logging.info(f"Reached max_tracks limit ({max_tracks}). Stopping.")
                        break
                else:
                    # No more pages
                    break

            except spotipy.SpotifyException as e:
                logging.error(f"Error fetching saved tracks (offset {offset}): {e}")
                # Depending on the error, you might want to retry or break
                break
            except Exception as e:
                logging.error(f"Unexpected error fetching saved tracks: {e}")
                break

        logging.info(f"Finished fetching saved tracks. Total retrieved: {len(saved_tracks)}")
        return saved_tracks

    def get_audio_features(self, track_ids):
        """
        Retrieves audio features for a list of track IDs.

        Args:
            track_ids (list): A list of Spotify track IDs (max 100 per request).

        Returns:
            list: A list of dictionaries containing audio features for each track ID.
                  Returns None if input is empty or an error occurs.
                  Features include danceability, energy, key, loudness, mode, speechiness,
                  acousticness, instrumentalness, liveness, valence, tempo.
        """
        if not self.sp:
            logging.error("Spotify client not initialized.")
            return None
        if not track_ids:
            logging.warning("No track IDs provided to get_audio_features.")
            return []

        features_list = []
        # Spotify API limit is 100 IDs per request for audio features
        chunk_size = 100
        for i in range(0, len(track_ids), chunk_size):
            chunk = track_ids[i:i + chunk_size]
            try:
                logging.info(f"Fetching audio features for {len(chunk)} tracks (batch {i//chunk_size + 1})...")
                features = self.sp.audio_features(tracks=chunk)
                # Filter out potential None results if a track ID was invalid
                valid_features = [f for f in features if f is not None]
                features_list.extend(valid_features)
            except spotipy.SpotifyException as e:
                logging.error(f"Error fetching audio features for batch starting at index {i}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching audio features: {e}")

        logging.info(f"Successfully fetched audio features for {len(features_list)} tracks.")
        return features_list

    # --- Add more methods as needed ---
    # e.g., get_recently_played, get_playlists, get_playlist_items

# --- Example Usage (for testing this script directly) ---
if __name__ == "__main__":
    logging.info("Running Spotify Client script directly for testing...")
    try:
        client = SpotifyClient()

        # Test getting saved tracks (limit to 10 for testing)
        print("\n--- Testing get_saved_tracks ---")
        tracks = client.get_saved_tracks(limit_per_req=5, max_tracks=10)
        if tracks:
            print(f"Successfully retrieved {len(tracks)} saved tracks.")
            print("First track:", tracks[0])
            track_ids = [t['id'] for t in tracks if t.get('id')] # Get IDs for next step

            # Test getting audio features for the retrieved tracks
            if track_ids:
                print("\n--- Testing get_audio_features ---")
                features = client.get_audio_features(track_ids)
                if features:
                    print(f"Successfully retrieved {len(features)} audio features.")
                    print("Features for first track:", features[0])
                else:
                    print("Could not retrieve audio features.")
            else:
                 print("No track IDs found to fetch features for.")

        else:
            print("Could not retrieve saved tracks.")

    except ValueError as ve:
        # Handle credential error specifically if needed
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
        logging.exception("Error during standalone script execution:")
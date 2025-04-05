# data_collection/spotify_client.py

import os
import logging
import json
import requests # <-- Import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- Environment Variable Check ---
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
FASTMCP_SERVER_URL = os.getenv("FASTMCP_SERVER_URL") # <-- Get MCP URL

if not all([SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI]):
    logging.error("Spotify API credentials not found in environment variables.")
    raise ValueError("Missing Spotify API credentials in .env file")
if not FASTMCP_SERVER_URL:
    logging.warning("FASTMCP_SERVER_URL not found in .env file. Data will not be sent.")
    # raise ValueError("Missing FASTMCP_SERVER_URL in .env file") # Or raise error if mandatory

SCOPE = "user-library-read playlist-read-private user-read-recently-played"

# --- Function to send data to MCP ---
def send_to_mcp(endpoint: str, data: list, server_url: str = FASTMCP_SERVER_URL):
    """Sends data payload to the specified MCP endpoint via POST request."""
    if not server_url:
        logging.warning(f"MCP Server URL not configured. Cannot send data to {endpoint}.")
        return False
    if not data:
        logging.info(f"No data provided to send to {endpoint}.")
        return True # Nothing to send is not an error

    url = f"{server_url.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {'Content-Type': 'application/json'}
    # Add authentication headers here if needed, e.g.:
    # headers['X-API-Key'] = 'YOUR_MCP_API_KEY'

    try:
        # Convert list of dictionaries to JSON string payload
        payload = json.dumps(data)
        response = requests.post(url, headers=headers, data=payload, timeout=30) # Added timeout
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        logging.info(f"Successfully sent {len(data)} items to MCP endpoint: {endpoint}")
        return True
    except requests.exceptions.ConnectionError as e:
        logging.error(f"MCP Connection Error: Could not connect to {url}. Error: {e}")
    except requests.exceptions.Timeout:
        logging.error(f"MCP Request Timeout: Timed out connecting to {url}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"MCP HTTP Error: Failed to send data to {url}. Status: {e.response.status_code}, Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"MCP Request Error: An error occurred sending data to {url}. Error: {e}")
    except json.JSONDecodeError as e:
         logging.error(f"JSON Error: Failed to encode data for sending to {url}. Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected Error sending data to MCP: {e}")

    return False

class SpotifyClient:
    """
    A client to interact with the Spotify Web API using Spotipy.
    Handles authentication and provides methods to fetch user data.
    Includes functionality to send collected data to a central MCP server.
    """
    def __init__(self, client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE):
        self.sp = None
        try:
            auth_manager = SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope=scope,
                cache_path=".spotify_token_cache"
            )
            self.sp = spotipy.Spotify(auth_manager=auth_manager)
            self.sp.current_user()
            logging.info("Successfully authenticated with Spotify.")
        except spotipy.SpotifyException as e:
            logging.error(f"Spotify authentication failed: {e}")
            print("\n>>> Spotify Auth Error <<<\nPlease ensure credentials are correct and run interactively once to authorize.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during Spotify client initialization: {e}")
            raise

    def get_saved_tracks(self, limit_per_req=50, max_tracks=None, send_data_to_mcp=True): # Added flag
        """ Fetches saved tracks and optionally sends them to the MCP server. """
        # ... (fetching logic remains the same as before) ...
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
                page_tracks = [] # Collect tracks for this page/batch
                for item in items:
                    track_info = item.get('track')
                    if track_info:
                        artists = [{'id': artist['id'], 'name': artist['name']} for artist in track_info['artists']]
                        album = {'id': track_info['album']['id'], 'name': track_info['album']['name']}
                        track_data = {
                            'id': track_info['id'],
                            'name': track_info['name'],
                            'artists': artists,
                            'album': album,
                            'duration_ms': track_info['duration_ms'],
                            'popularity': track_info['popularity'],
                            'external_url': track_info['external_urls']['spotify'],
                            'added_at': item['added_at']
                        }
                        saved_tracks.append(track_data)
                        page_tracks.append(track_data) # Add to page batch
                        total_fetched += 1

                logging.info(f"Fetched {len(items)} tracks. Total fetched so far: {total_fetched}")

                # --- Send this page/batch of tracks to MCP ---
                if send_data_to_mcp and page_tracks:
                    send_to_mcp("/submit/spotify/tracks", page_tracks)
                # ---------------------------------------------

                if results.get('next'):
                    offset += limit_per_req
                    if max_tracks is not None and total_fetched >= max_tracks:
                        logging.info(f"Reached max_tracks limit ({max_tracks}). Stopping.")
                        break
                else:
                    break

            except spotipy.SpotifyException as e:
                logging.error(f"Error fetching saved tracks (offset {offset}): {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error fetching saved tracks: {e}")
                break

        logging.info(f"Finished fetching saved tracks. Total retrieved: {len(saved_tracks)}")
        # Return all collected tracks regardless of sending success
        return saved_tracks

    def get_audio_features(self, track_ids, send_data_to_mcp=True): # Added flag
        """ Fetches audio features and optionally sends them to the MCP server. """
        # ... (fetching logic remains the same) ...
        if not self.sp:
            logging.error("Spotify client not initialized.")
            return None
        if not track_ids:
            logging.warning("No track IDs provided to get_audio_features.")
            return []

        features_list = []
        chunk_size = 100
        for i in range(0, len(track_ids), chunk_size):
            chunk = track_ids[i:i + chunk_size]
            batch_features = [] # Collect features for this batch
            try:
                logging.info(f"Fetching audio features for {len(chunk)} tracks (batch {i//chunk_size + 1})...")
                features = self.sp.audio_features(tracks=chunk)
                valid_features = [f for f in features if f is not None]
                batch_features.extend(valid_features) # Add to batch
                features_list.extend(valid_features) # Add to overall list

                # --- Send this batch of features to MCP ---
                if send_data_to_mcp and batch_features:
                    send_to_mcp("/submit/spotify/features", batch_features)
                # ------------------------------------------

            except spotipy.SpotifyException as e:
                logging.error(f"Error fetching audio features for batch starting at index {i}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error fetching audio features: {e}")

        logging.info(f"Successfully fetched audio features for {len(features_list)} tracks.")
        return features_list


# --- Example Usage (Updated for MCP) ---
if __name__ == "__main__":
    logging.info("Running Spotify Client script directly for testing...")
    if not FASTMCP_SERVER_URL:
         print("!!! Warning: FASTMCP_SERVER_URL is not set in .env. Data cannot be sent. Testing fetch only.")

    try:
        client = SpotifyClient()

        print("\n--- Testing get_saved_tracks (sending to MCP enabled by default) ---")
        # Fetch a small number, data should be sent in batches by the function itself
        tracks = client.get_saved_tracks(limit_per_req=5, max_tracks=10, send_data_to_mcp=True)
        if tracks:
            print(f"Retrieved {len(tracks)} tracks locally (check logs for MCP send status).")
            track_ids = [t['id'] for t in tracks if t.get('id')]

            if track_ids:
                print("\n--- Testing get_audio_features (sending to MCP enabled by default) ---")
                features = client.get_audio_features(track_ids, send_data_to_mcp=True)
                if features:
                    print(f"Retrieved {len(features)} audio features locally (check logs for MCP send status).")
                else:
                    print("Could not retrieve audio features.")
            else:
                 print("No track IDs found to fetch features for.")
        else:
            print("Could not retrieve saved tracks.")

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
        logging.exception("Error during standalone script execution:")
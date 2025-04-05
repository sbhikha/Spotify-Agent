# data_collection/lastfm_client.py

import os
import logging
import json # <-- Import json
import requests # <-- Import requests
import pylast
from dotenv import load_dotenv
import time

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- Environment Variable Check ---
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_API_SECRET = os.getenv("LASTFM_API_SECRET")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME")
FASTMCP_SERVER_URL = os.getenv("FASTMCP_SERVER_URL") # <-- Get MCP URL

if not all([LASTFM_API_KEY, LASTFM_API_SECRET, LASTFM_USERNAME]):
    logging.error("Last.fm API credentials not found in environment variables.")
    raise ValueError("Missing Last.fm API credentials in .env file")
if not FASTMCP_SERVER_URL:
    logging.warning("FASTMCP_SERVER_URL not found in .env file. Data will not be sent.")

# --- Function to send data to MCP (can be shared via a util module later) ---
def send_to_mcp(endpoint: str, data: list, server_url: str = FASTMCP_SERVER_URL):
    """Sends data payload to the specified MCP endpoint via POST request."""
    if not server_url:
        logging.warning(f"MCP Server URL not configured. Cannot send data to {endpoint}.")
        return False
    if not data:
        logging.info(f"No data provided to send to {endpoint}.")
        return True

    url = f"{server_url.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {'Content-Type': 'application/json'}
    # Add authentication headers here if needed

    try:
        # Need to ensure datetime objects are handled - Pylast might return them.
        # The current fetching logic converts timestamp to int/string, which is fine.
        payload = json.dumps(data)
        response = requests.post(url, headers=headers, data=payload, timeout=30)
        response.raise_for_status()
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


class LastFMClient:
    """
    A client to interact with the Last.fm API using Pylast.
    Includes functionality to send collected data to a central MCP server.
    """
    def __init__(self, api_key=LASTFM_API_KEY, api_secret=LASTFM_API_SECRET, username=LASTFM_USERNAME):
        # ... (Initialization logic remains the same) ...
        self.network = None
        self.username = username
        try:
            self.network = pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)
            logging.info("Successfully initialized Last.fm network connection.")
            self.get_user() # Check if username is valid
        except pylast.WSError as e:
            logging.error(f"Failed to initialize Last.fm network. Error: {e}")
            print(f"\n>>> Last.fm API Error <<<\nPlease ensure API Key/Secret are correct. Error: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during Last.fm client initialization: {e}")
            raise

    def get_user(self):
        # ... (Remains the same) ...
        if not self.network:
            logging.error("Last.fm network not initialized.")
            return None
        try:
            user = self.network.get_user(self.username)
            # Don't log success here every time it's called internally
            return user
        except pylast.WSError as e:
            logging.error(f"Could not get Last.fm user '{self.username}'. Is the username correct? Error: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting Last.fm user: {e}")
            return None

    def get_recent_tracks(self, limit=200, max_pages=None, time_from=None, time_to=None, send_data_to_mcp=True): # Added flag
        """ Fetches recent tracks and optionally sends them to the MCP server page by page. """
        user = self.get_user()
        if not user:
            return []

        all_recent_tracks = [] # Collect all tracks locally too
        page = 1
        total_fetched = 0
        processed_pages = 0

        logging.info(f"Fetching recent tracks for '{self.username}' from Last.fm (limit={limit}, max_pages={max_pages or 'All'})...")

        while True:
            if max_pages is not None and processed_pages >= max_pages:
                logging.info(f"Reached max_pages limit ({max_pages}). Stopping.")
                break

            page_tracks = [] # Collect tracks for this page/batch
            try:
                results = user.get_recent_tracks(
                    limit=limit,
                    page=page,
                    time_from=time_from,
                    time_to=time_to,
                    stream=False
                )

                if not results:
                    logging.info(f"No more tracks found on page {page}.")
                    break

                for item in results:
                    album_name = item.track.get_album().get_title() if item.track.get_album() else None
                    track_data = {
                        'artist': item.track.artist.name,
                        'title': item.track.title,
                        'album': album_name,
                        'timestamp_uts': int(item.playback_date.timestamp()),
                        'datetime_utc': item.playback_date.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    all_recent_tracks.append(track_data)
                    page_tracks.append(track_data) # Add to page batch
                    total_fetched += 1

                logging.info(f"Fetched {len(page_tracks)} tracks from page {page}. Total fetched so far: {total_fetched}")

                # --- Send this page/batch of tracks to MCP ---
                if send_data_to_mcp and page_tracks:
                    send_to_mcp("/submit/lastfm/scrobbles", page_tracks)
                # ---------------------------------------------

                processed_pages += 1

                if len(page_tracks) < limit:
                     logging.info("Received fewer tracks than limit, assuming end of results.")
                     break

                page += 1
                time.sleep(0.2) # Basic rate limiting

            except pylast.WSError as e:
                logging.error(f"Last.fm API error on page {page}: {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error fetching recent tracks on page {page}: {e}")
                break

        logging.info(f"Finished fetching recent tracks. Total retrieved: {len(all_recent_tracks)}")
        return all_recent_tracks # Return all collected tracks


# --- Example Usage (Updated for MCP) ---
if __name__ == "__main__":
    logging.info("Running Last.fm Client script directly for testing...")
    if not FASTMCP_SERVER_URL:
         print("!!! Warning: FASTMCP_SERVER_URL is not set in .env. Data cannot be sent. Testing fetch only.")

    try:
        client = LastFMClient()

        print("\n--- Testing get_recent_tracks (sending to MCP enabled by default) ---")
        # Fetch only 1 page with up to 10 tracks for testing
        tracks = client.get_recent_tracks(limit=10, max_pages=1, send_data_to_mcp=True)

        if tracks:
            print(f"Retrieved {len(tracks)} recent tracks locally (check logs for MCP send status).")
        else:
            print(f"Could not retrieve recent tracks for user '{client.username}'. Check username and API keys.")

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
        logging.exception("Error during standalone script execution:")
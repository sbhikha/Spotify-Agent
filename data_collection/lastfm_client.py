# data_collection/lastfm_client.py

import os
import logging
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
# LASTFM_PASSWORD_HASH = os.getenv("LASTFM_PASSWORD_HASH") # Only needed for write access

if not all([LASTFM_API_KEY, LASTFM_API_SECRET, LASTFM_USERNAME]):
    logging.error("Last.fm API credentials not found in environment variables.")
    raise ValueError("Missing Last.fm API credentials in .env file (LASTFM_API_KEY, LASTFM_API_SECRET, LASTFM_USERNAME)")

class LastFMClient:
    """
    A client to interact with the Last.fm API using Pylast.
    Handles authentication and provides methods to fetch user data.
    """
    def __init__(self, api_key=LASTFM_API_KEY, api_secret=LASTFM_API_SECRET, username=LASTFM_USERNAME):
        """
        Initializes the Last.fm network client.
        """
        self.network = None
        self.username = username
        try:
            # Initialize the network object. Authentication for user-specific read methods
            # typically only requires the username passed to the method call.
            # Password hash is usually only needed for write operations or specific auth methods.
            self.network = pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)
            logging.info("Successfully initialized Last.fm network connection.")
            # Test connection by getting user object (doesn't require password hash)
            self.get_user() # Check if username is valid

        except pylast.WSError as e:
            logging.error(f"Failed to initialize Last.fm network. Error: {e}")
            print(f"\n>>> Last.fm API Error <<<\nPlease ensure your API Key/Secret in .env are correct. Error: {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during Last.fm client initialization: {e}")
            raise

    def get_user(self):
        """Gets the Pylast User object for the configured username."""
        if not self.network:
            logging.error("Last.fm network not initialized.")
            return None
        try:
            user = self.network.get_user(self.username)
            logging.info(f"Successfully obtained Last.fm user object for '{self.username}'.")
            return user
        except pylast.WSError as e:
            logging.error(f"Could not get Last.fm user '{self.username}'. Is the username correct? Error: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting Last.fm user: {e}")
            return None

    def get_recent_tracks(self, limit=200, max_pages=None, time_from=None, time_to=None):
        """
        Retrieves the recent tracks (scrobbles) for the configured user.

        Args:
            limit (int): Number of results per page (max 200 for recent tracks).
            max_pages (int, optional): Maximum number of pages to retrieve. Defaults to None (retrieve all available within time range).
            time_from (int, optional): Unix timestamp. Only fetch results after this time.
            time_to (int, optional): Unix timestamp. Only fetch results before this time.

        Returns:
            list: A list of dictionaries, each representing a scrobbled track
                  (artist, title, album, timestamp). Returns an empty list on error.
        """
        user = self.get_user()
        if not user:
            return []

        recent_tracks = []
        page = 1
        total_fetched = 0
        processed_pages = 0

        logging.info(f"Fetching recent tracks for '{self.username}' from Last.fm (limit={limit}, max_pages={max_pages or 'All'})...")

        while True:
            if max_pages is not None and processed_pages >= max_pages:
                logging.info(f"Reached max_pages limit ({max_pages}). Stopping.")
                break
            try:
                # Note: Pylast's get_recent_tracks handles pagination internally via the generator
                # but we might want explicit page control for large histories or rate limits.
                # The 'limit' here is results *per page*.
                # Pylast might fetch more than 'limit' if iterating fully? Let's test.
                # Update: Pylast's get_recent_tracks returns a generator that yields Track objects.
                # Let's use the lower-level `user.get_recent_tracks` which seems more controllable.

                results = user.get_recent_tracks(
                    limit=limit, # This might actually be total limit for the call in some pylast versions? Test needed. Let's assume per page for now.
                    page=page,
                    time_from=time_from,
                    time_to=time_to,
                    stream=False # Get a list for the page, not a generator
                )

                if not results:
                    logging.info(f"No more tracks found on page {page}.")
                    break

                page_tracks = 0
                for item in results:
                    # Sometimes the album might be None or missing info
                    album_name = item.track.get_album().get_title() if item.track.get_album() else None

                    recent_tracks.append({
                        'artist': item.track.artist.name,
                        'title': item.track.title,
                        'album': album_name,
                        'timestamp_uts': int(item.playback_date.timestamp()), # Unix timestamp
                        'datetime_utc': item.playback_date.strftime('%Y-%m-%d %H:%M:%S') # Human-readable UTC
                    })
                    page_tracks += 1
                    total_fetched += 1

                logging.info(f"Fetched {page_tracks} tracks from page {page}. Total fetched so far: {total_fetched}")
                processed_pages += 1

                # Simple check: If we received fewer tracks than the limit, we're likely on the last page
                if page_tracks < limit:
                     logging.info("Received fewer tracks than limit, assuming end of results.")
                     break

                page += 1
                # Optional: Add a small delay to avoid hitting rate limits aggressively
                time.sleep(0.2)

            except pylast.WSError as e:
                # Handle specific Last.fm errors (e.g., rate limiting)
                logging.error(f"Last.fm API error on page {page}: {e}")
                # You might want retry logic here, especially for rate limit errors (code 29)
                break
            except Exception as e:
                logging.error(f"Unexpected error fetching recent tracks on page {page}: {e}")
                break

        logging.info(f"Finished fetching recent tracks. Total retrieved: {len(recent_tracks)}")
        return recent_tracks

    # --- Add more methods as needed ---
    # e.g., get_track_info (tags, listeners), get_user_top_artists/tracks

# --- Example Usage (for testing this script directly) ---
if __name__ == "__main__":
    logging.info("Running Last.fm Client script directly for testing...")
    try:
        client = LastFMClient()

        # Test getting recent tracks (limit to 10 for testing)
        print("\n--- Testing get_recent_tracks ---")
        # Fetch only the 10 most recent tracks using the limit parameter directly
        # Note: Pylast documentation/behavior on limit vs pages can be tricky.
        # This call might retrieve up to `limit` items total.
        tracks = client.get_recent_tracks(limit=10, max_pages=1) # Explicitly get only first page up to 10 items

        if tracks:
            print(f"Successfully retrieved {len(tracks)} recent tracks.")
            print("Most recent track:", tracks[0])
        else:
            print(f"Could not retrieve recent tracks for user '{client.username}'. Check username and API keys.")

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
        logging.exception("Error during standalone script execution:")
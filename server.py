# mcp_server.py

import logging
from typing import List, Optional, Dict, Any

# Assuming your client files are in a 'data_collection' subdirectory
# Adjust the import path if your file structure is different
try:
    from data_collection.lastfm_client import LastFMClient
    from data_collection.spotify_client import SpotifyClient
except ImportError:
    logging.error("Could not import client classes. Make sure lastfm_client.py and spotify_client.py are accessible (e.g., in a 'data_collection' folder).")
    # Optionally re-raise or exit if clients are essential
    raise

from mcp.server.fastmcp import FastMCP

# --- Initialize Clients ---
# It's good practice to initialize clients once when the server starts.
# Handle potential errors during initialization (e.g., missing credentials)
try:
    lastfm = LastFMClient()
    logging.info("Last.fm client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Last.fm client: {e}", exc_info=True)
    # Decide how to handle: exit, run without lastfm, etc.
    # For this example, we'll allow the server to start but log the error.
    # Tools using this client will likely fail.
    lastfm = None

try:
    spotify = SpotifyClient()
    logging.info("Spotify client initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Spotify client: {e}", exc_info=True)
    # Decide how to handle: exit, run without spotify, etc.
    spotify = None


# --- Create an MCP Server ---
mcp = FastMCP("MusicDataCollector")
logging.info("MCP Server 'MusicDataCollector' created.")

# --- Add Last.fm Tools ---

@mcp.tool()
def get_lastfm_recent_tracks(limit: int = 50, max_pages: Optional[int] = None, time_from: Optional[int] = None, time_to: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Retrieves the recent tracks (scrobbles) for the configured Last.fm user.

    Args:
        limit (int): Number of results per page (max 200 for recent tracks, default 50).
        max_pages (int, optional): Maximum number of pages to retrieve. Defaults to None (retrieve all available within time range).
        time_from (int, optional): Unix timestamp. Only fetch results after this time.
        time_to (int, optional): Unix timestamp. Only fetch results before this time.

    Returns:
        list: A list of dictionaries, each representing a scrobbled track
              (artist, title, album, timestamp_uts, datetime_utc). Returns empty list on error or if client not initialized.
    """
    if not lastfm:
        logging.error("Last.fm client is not available for get_lastfm_recent_tracks tool.")
        return []
    try:
        return lastfm.get_recent_tracks(limit=limit, max_pages=max_pages, time_from=time_from, time_to=time_to)
    except Exception as e:
        logging.error(f"Error calling get_lastfm_recent_tracks tool: {e}", exc_info=True)
        return [] # Return empty list on error to avoid breaking agent flow

# --- Add Spotify Tools ---

@mcp.tool()
def get_spotify_saved_tracks(limit_per_req: int = 50, max_tracks: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Retrieves the current user's saved tracks (Liked Songs) from Spotify.

    Args:
        limit_per_req (int): Number of tracks to fetch per API request (max 50, default 50).
        max_tracks (int, optional): Maximum total tracks to retrieve. Defaults to None (retrieve all).

    Returns:
        list: A list of dictionaries, each containing information about a saved track
              (id, name, artists, album, added_at, duration_ms, popularity, external_url).
              Returns empty list on error or if client not initialized.
    """
    if not spotify:
        logging.error("Spotify client is not available for get_spotify_saved_tracks tool.")
        return []
    try:
        return spotify.get_saved_tracks(limit_per_req=limit_per_req, max_tracks=max_tracks)
    except Exception as e:
        logging.error(f"Error calling get_spotify_saved_tracks tool: {e}", exc_info=True)
        return [] # Return empty list on error

@mcp.tool()
def get_spotify_audio_features(track_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Retrieves audio features for a list of Spotify track IDs.

    Args:
        track_ids (list): A list of Spotify track IDs (max 100 per internal batch).

    Returns:
        list: A list of dictionaries containing audio features for each track ID found.
              Features include danceability, energy, key, loudness, mode, speechiness,
              acousticness, instrumentalness, liveness, valence, tempo, id, uri, etc.
              Returns empty list on error, if no IDs provided, or if client not initialized.
    """
    if not spotify:
        logging.error("Spotify client is not available for get_spotify_audio_features tool.")
        return []
    if not track_ids:
        logging.warning("No track IDs provided to get_spotify_audio_features tool.")
        return []
    try:
        # Ensure the input is actually a list, sometimes LLMs might pass a single string
        if isinstance(track_ids, str):
             # Attempt to handle simple cases, like comma-separated or space-separated IDs
             # More robust parsing might be needed depending on expected LLM behavior
             track_ids = [tid.strip() for tid in track_ids.replace(',', ' ').split() if tid.strip()]
             logging.warning(f"Received string for track_ids, attempting to parse as list: {track_ids}")
        elif not isinstance(track_ids, list):
             logging.error(f"Invalid type for track_ids: {type(track_ids)}. Expected List[str].")
             return []

        return spotify.get_audio_features(track_ids=track_ids)
    except Exception as e:
        logging.error(f"Error calling get_spotify_audio_features tool: {e}", exc_info=True)
        return [] # Return empty list on error

# --- (Optional) Add more tools or resources as needed ---


# --- Run the MCP Server ---
if __name__ == "__main__":
    logging.info("Starting MCP Server...")
    # Check if clients were initialized before trying to run
    if not lastfm:
        logging.warning("Running MCP server without functional Last.fm client.")
    if not spotify:
        logging.warning("Running MCP server without functional Spotify client.")

    # The run command will typically start a web server (like FastAPI/Uvicorn)
    # to expose the tools via an API.
    # Make sure you have the necessary dependencies installed (e.g., uvicorn, fastapi)
    # often handled by installing the mcp library itself.
    try:
      mcp.run()
      # Example: mcp.run(host="0.0.0.0", port=8000) # if specific host/port needed
    except Exception as e:
        logging.critical(f"MCP Server failed to start or crashed: {e}", exc_info=True)
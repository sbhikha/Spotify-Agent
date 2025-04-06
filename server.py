# server.py
import os
import logging
from typing import List, Dict, Any, Optional

# Assuming 'mcp.server.fastmcp' is the correct path based on the example
# Replace with the actual import if different
from mcp.server.fastmcp import FastMCP

# Import your client classes
from data_collection.spotify_client import SpotifyClient
from data_collection.lastfm_client import LastFMClient

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Ensure .env file is loaded (clients might do this, but good practice here too)
from dotenv import load_dotenv
load_dotenv()

# --- MCP Server Setup ---
mcp = FastMCP("MusicDataCollector")

# --- Initialize Clients ---
# It's good practice to initialize clients once when the server starts.
# Handle potential errors during initialization (e.g., missing credentials)
try:
    lastfm_client = LastFMClient() # Uses env vars by default
    logging.info("LastFMClient initialized successfully.")
    # Check Last.fm connection
    if not lastfm_client.get_user(): # Checks user validity
         logging.error(f"Could not validate Last.fm user '{lastfm_client.username}'. Check username and API keys.")
         lastfm_client = None # Mark as unavailable
    else:
        logging.info(f"Successfully connected to Last.fm for user '{lastfm_client.username}'.")

except ValueError as e:
    logging.error(f"Failed to initialize LastFMClient: {e}")
    lastfm_client = None # Mark as unavailable
except Exception as e:
    logging.error(f"An unexpected error occurred initializing LastFMClient: {e}")
    lastfm_client = None

try:
    spotify_client = SpotifyClient()
    logging.info("SpotifyClient initialized successfully.")
    # Optionally, call ensure_token_valid if needed, though Spotipy often handles this
    # spotify_client.ensure_token_valid()
    # logging.info("Spotify token checked/refreshed.")
except ValueError as e:
    logging.error(f"Failed to initialize SpotifyClient: {e}")
    spotify_client = None # Mark as unavailable
except Exception as e:
    logging.error(f"An unexpected error occurred initializing SpotifyClient: {e}")
    spotify_client = None
# Check Spotify Authentication (Optional but recommended)
if spotify_client:
    try:
        profile = spotify_client.get_user_profile()
        logging.info(f"Successfully authenticated with Spotify as user: {profile.get('display_name', 'N/A')}")
    except Exception as e:
        logging.error(f"Spotify authentication check failed. Client may not be usable. Error: {e}")
        # Decide if you want to disable spotify_client here or let tools fail
        # spotify_client = None # Uncomment to disable if auth fails

# --- Spotify Tools ---

if spotify_client:
    @mcp.tool()
    def get_spotify_user_profile() -> Dict[str, Any]:
        """Gets the current authenticated Spotify user's profile information."""
        return spotify_client.get_user_profile()

    @mcp.tool()
    def get_spotify_saved_tracks() -> List[Dict[str, Any]]:
        """Retrieves all of the current user's saved tracks (Liked Songs) from Spotify."""
        return spotify_client.get_saved_tracks()

    @mcp.tool()
    def get_spotify_track_audio_features(track_ids: List[str]) -> List[Dict[str, Any]]:
        """Retrieves audio features (danceability, energy, etc.) for a list of Spotify track IDs (max 100 per batch internally)."""
        return spotify_client.get_track_audio_features(track_ids=track_ids)

    @mcp.tool()
    def get_spotify_user_top_tracks(time_range: str = "medium_term", limit: int = 50) -> List[Dict[str, Any]]:
        """Gets the user's top tracks from Spotify for a given time range ('short_term', 'medium_term', 'long_term')."""
        if time_range not in ["short_term", "medium_term", "long_term"]:
            raise ValueError("time_range must be one of 'short_term', 'medium_term', 'long_term'")
        return spotify_client.get_user_top_tracks(time_range=time_range, limit=limit)

    @mcp.tool()
    def get_spotify_user_top_artists(time_range: str = "medium_term", limit: int = 50) -> List[Dict[str, Any]]:
        """Gets the user's top artists from Spotify for a given time range ('short_term', 'medium_term', 'long_term')."""
        if time_range not in ["short_term", "medium_term", "long_term"]:
            raise ValueError("time_range must be one of 'short_term', 'medium_term', 'long_term'")
        return spotify_client.get_user_top_artists(time_range=time_range, limit=limit)

    @mcp.tool()
    def get_spotify_recently_played(limit: int = 50) -> List[Dict[str, Any]]:
        """Gets the user's recently played tracks from Spotify."""
        return spotify_client.get_recently_played(limit=limit)

    @mcp.tool()
    def search_spotify(query: str, types: List[str] = ["track"], limit: int = 10) -> Dict[str, Any]:
        """Searches for items on Spotify. Types can include 'track', 'artist', 'album', 'playlist'."""
        valid_types = {'track', 'artist', 'album', 'playlist', 'show', 'episode'}
        if not all(t in valid_types for t in types):
             raise ValueError(f"Invalid type specified. Allowed types: {valid_types}")
        return spotify_client.search(query=query, types=types, limit=limit)

    @mcp.tool()
    def get_spotify_user_playlists(limit: int = 50) -> List[Dict[str, Any]]:
        """Get the current user's playlists."""
        return spotify_client.get_user_playlists(limit=limit)

    @mcp.tool()
    def get_spotify_playlist_tracks(playlist_id: str) -> List[Dict[str, Any]]:
        """Get all tracks from a specific Spotify playlist."""
        return spotify_client.get_playlist_tracks(playlist_id=playlist_id)

    # Add more wrapped Spotify methods as needed (create_playlist, add_tracks_to_playlist etc.)
    # Be mindful of exposing methods that modify data if that's not intended for all users of the MCP.

else:
    logging.warning("SpotifyClient failed to initialize. Spotify tools will not be available.")


# --- Last.fm Tools ---

if lastfm_client:
    @mcp.tool()
    def get_lastfm_recent_tracks(limit_per_page: int = 50, max_pages: Optional[int] = 4, time_from: Optional[int] = None, time_to: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieves recent tracks (scrobbles) for the configured Last.fm user.
        Args:
            limit_per_page (int): Max tracks per page request (Last.fm max is 200, default 50).
            max_pages (int, optional): Max number of pages to fetch (default 4 to limit calls). Set to None for potentially all.
            time_from (int, optional): Unix timestamp. Only fetch results after this time.
            time_to (int, optional): Unix timestamp. Only fetch results before this time.
        Returns:
            List of track dictionaries.
        """
        # Clamp limit_per_page to Last.fm's max
        actual_limit = min(limit_per_page, 200)
        if limit_per_page > 200:
             logging.warning("Requested limit_per_page > 200, using 200 (Last.fm max).")

        return lastfm_client.get_recent_tracks(
            limit=actual_limit,
            max_pages=max_pages,
            time_from=time_from,
            time_to=time_to
        )

    # Add wrappers for other LastFMClient methods if needed
    # e.g., get_lastfm_user_top_artists, get_lastfm_track_info

else:
    logging.warning("LastFMClient failed to initialize or validate user. Last.fm tools will not be available.")


# --- Example Resource (from original image, if needed) ---
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


# --- Running the Server ---
# This part depends on how FastMCP is designed to run.
# It might be integrated with FastAPI/Uvicorn.
# Example using a hypothetical run method:
if __name__ == "__main__":
    logging.info("Starting MCP server...")
    # This line is hypothetical, replace with actual run command for FastMCP
    # e.g., mcp.run(host="0.0.0.0", port=8000)
    # Or, if FastMCP is a FastAPI app:
    # import uvicorn
    # uvicorn.run(mcp.app, host="0.0.0.0", port=8000) # Assuming mcp has a .app attribute

    # Placeholder if run command is unknown:
    print("\n" + "="*30)
    print("MCP Server Ready (Simulated Run)")
    print(f"Initialized MCP: {mcp.name}")
    print("Available Tools:")
    for name, tool in mcp.tools.items(): # Assuming mcp has a 'tools' attribute
        print(f"- {name}: {tool.description}") # Assuming tool has a description/docstring
    print("Available Resources:")
    for path, resource in mcp.resources.items(): # Assuming mcp has a 'resources' attribute
        print(f"- {path}: {resource.description}")
    print("="*30)
    print("\nNOTE: Replace the `if __name__ == '__main__':` block with the actual command to run your FastMCP server.")
# data_collection/spotify_client.py
import os
import time
from typing import Dict, List, Any, Optional
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

class SpotifyClient:
    """
    Client for interacting with the Spotify API using Spotipy.
    Handles authentication and various API endpoints.
    """
    def __init__(self, client_id: str = None, client_secret: str = None, redirect_uri: str = None):
        # Try to get credentials from environment variables if not provided
        self.client_id = client_id or os.environ.get('SPOTIPY_CLIENT_ID')
        self.client_secret = client_secret or os.environ.get('SPOTIPY_CLIENT_SECRET')
        self.redirect_uri = redirect_uri or os.environ.get('SPOTIPY_REDIRECT_URI')
        
        # Verify credentials are available
        if not (self.client_id and self.client_secret and self.redirect_uri):
            raise ValueError(
                "Spotify credentials not found. Either pass them directly or set "
                "SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, and SPOTIFY_REDIRECT_URI environment variables."
            )
        
        # Define scopes
        self.scope = "user-library-read playlist-read-private playlist-read-collaborative playlist-modify-private playlist-modify-public user-top-read user-read-recently-played"
        
        # Initialize Spotipy client
        self.sp_oauth = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope=self.scope
        )
        self.sp = spotipy.Spotify(auth_manager=self.sp_oauth)
        
    def get_auth_url(self) -> str:
        """Generate the Spotify authorization URL."""
        return self.sp_oauth.get_authorize_url()
    
    def request_tokens(self, code: str) -> Dict[str, Any]:
        """Exchange authorization code for access and refresh tokens."""
        token_info = self.sp_oauth.get_access_token(code)
        return token_info
    
    def refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the access token using refresh token."""
        token_info = self.sp_oauth.refresh_access_token(self.sp_oauth.refresh_token)
        return token_info
    
    def ensure_token_valid(self) -> None:
        """Check if the access token is valid, refresh if needed."""
        if self.sp_oauth.is_token_expired(self.sp_oauth.get_cached_token()):
            self.refresh_access_token()
    
    def get_user_profile(self) -> Dict[str, Any]:
        """Get the current user's profile."""
        return self.sp.current_user()
    
    def get_user_playlists(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the current user's playlists."""
        playlists = []
        offset = 0
        
        while True:
            results = self.sp.current_user_playlists(limit=limit, offset=offset)
            playlists.extend(results['items'])
            
            if len(results['items']) < limit or not results['next']:
                break
            
            offset += limit
        
        return playlists
    
    def get_playlist_tracks(self, playlist_id: str) -> List[Dict[str, Any]]:
        """Get all tracks from a playlist."""
        tracks = []
        offset = 0
        limit = 100
        
        while True:
            results = self.sp.playlist_items(
                playlist_id, 
                offset=offset,
                limit=limit,
                fields='items(track(id,name,artists,album)),next'
            )
            tracks.extend(results['items'])
            
            if len(results['items']) < limit or not results.get('next'):
                break
            
            offset += limit
        
        return tracks
    
    def get_saved_tracks(self) -> List[Dict[str, Any]]:
        """Get all the user's saved tracks."""
        tracks = []
        offset = 0
        limit = 50
        
        while True:
            results = self.sp.current_user_saved_tracks(limit=limit, offset=offset)
            tracks.extend(results['items'])
            
            if len(results['items']) < limit or not results.get('next'):
                break
            
            offset += limit
        
        return tracks
    
    def get_track_audio_features(self, track_ids: List[str]) -> List[Dict[str, Any]]:
        """Get audio features for multiple tracks."""
        features = []
        # Process in batches of 100 (Spotify API limit)
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i+100]
            features.extend(self.sp.audio_features(batch))
        
        return features
    
    def get_track_audio_analysis(self, track_id: str) -> Dict[str, Any]:
        """Get detailed audio analysis for a single track."""
        return self.sp.audio_analysis(track_id)
    
    def get_user_top_tracks(self, time_range: str = "medium_term", limit: int = 50) -> List[Dict[str, Any]]:
        """Get the user's top tracks. Time range can be long_term, medium_term, or short_term."""
        return self.sp.current_user_top_tracks(
            time_range=time_range, 
            limit=limit
        )['items']
    
    def get_user_top_artists(self, time_range: str = "medium_term", limit: int = 50) -> List[Dict[str, Any]]:
        """Get the user's top artists. Time range can be long_term, medium_term, or short_term."""
        return self.sp.current_user_top_artists(
            time_range=time_range, 
            limit=limit
        )['items']
    
    def create_playlist(self, name: str, description: str = "", public: bool = False) -> Dict[str, Any]:
        """Create a new playlist for the current user."""
        user_id = self.get_user_profile()["id"]
        return self.sp.user_playlist_create(
            user=user_id,
            name=name,
            public=public,
            description=description
        )
    
    def add_tracks_to_playlist(self, playlist_id: str, track_uris: List[str]) -> Dict[str, Any]:
        """Add tracks to a playlist."""
        responses = []
        # Process in batches of 100 (Spotify API limit)
        for i in range(0, len(track_uris), 100):
            batch = track_uris[i:i+100]
            response = self.sp.playlist_add_items(playlist_id, batch)
            responses.append(response)
        
        return responses[-1] if responses else {}
    
    def get_recently_played(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the user's recently played tracks."""
        return self.sp.current_user_recently_played(limit=limit)['items']
    
    def search(self, query: str, types: List[str], limit: int = 10) -> Dict[str, Any]:
        """Search for items on Spotify."""
        types_str = ",".join(types)
        return self.sp.search(q=query, type=types_str, limit=limit)
from datetime import datetime
from .config import ACTIVE_SESSION_KEY
import requests

def get_active_session_key():
    """
    Returns the session key defined in the central config.
    Used by all DAGs to ensure data consistency across the pipeline.
    """
    return ACTIVE_SESSION_KEY

def get_start_and_end_time(session_key):
    """
    Fetches the actual start and end timestamps for a specific F1 session.
    Works for any session_key (Testing, Practice, Qualifying, Race).
    """

    url = "https://api.openf1.org/v1/sessions"
    params = {"session_key": session_key}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"❌ API returned no data for session {session_key}")
            return None, None

        start_str = data[0]['date_start']
        end_str = data[0]['date_end']

        start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00'))

        return start_time, end_time
    except Exception as e:
        print(f"🚨 Function Error: {e}")
        return None, None
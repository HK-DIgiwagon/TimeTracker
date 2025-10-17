import requests
from dotenv import load_dotenv, set_key
import os
from datetime import datetime, timedelta, timezone

# Load .env
load_dotenv()
ENV_FILE = ".env"

def get_and_store_access_token():
    """
    Calls Zoho token URL from .env, stores the access token and expiry time in .env.
    """
    token_url = os.getenv("TOKEN_URL")
    if not token_url:
        raise ValueError("TOKEN_URL not found in .env")

    response = requests.post(token_url)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in")  # usually in seconds

        if access_token and expires_in:
            # Calculate expiry datetime using timezone-aware UTC
            expiry_time = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

            # Store access token and expiry in .env
            set_key(ENV_FILE, "ZOHO_ACCESS_TOKEN", access_token)
            set_key(ENV_FILE, "ZOHO_TOKEN_EXPIRY", expiry_time.isoformat())
            print("Access token and expiry time saved successfully in .env")
            return access_token
        else:
            raise ValueError("Access token or expires_in not found in response")
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")


def get_valid_access_token():
    """
    Returns a valid access token, refreshes it if expired.
    """
    access_token = os.getenv("ZOHO_ACCESS_TOKEN")
    expiry_str = os.getenv("ZOHO_TOKEN_EXPIRY")

    if access_token and expiry_str:
        expiry_time = datetime.fromisoformat(expiry_str)
        # Ensure expiry_time is timezone-aware
        if expiry_time.tzinfo is None:
            expiry_time = expiry_time.replace(tzinfo=timezone.utc)
        
        if datetime.now(timezone.utc) < expiry_time:
            # Token still valid
            return access_token

    # Token missing or expired, refresh
    return get_and_store_access_token()


# Example usage
if __name__ == "__main__":
    token = get_valid_access_token()
    print("Using Access Token:", token)
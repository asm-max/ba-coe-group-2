"""
celonis_connection.py
---------------------
A reusable connection function with proper error handling.
Can be imported into any script or run standalone to test the connection.
"""

import os
from dotenv import load_dotenv
from pycelonis import get_celonis

# Load credentials from .env
load_dotenv()


def get_connection(
    base_url: str  = None,
    api_token: str = None,
    key_type: str  = "USER_KEY"
):
    """
    Create and return an authenticated Celonis connection.

    Args:
        base_url  : Celonis tenant URL. Falls back to .env BASE_URL if not provided.
        api_token : Celonis API token. Falls back to .env API_TOKEN if not provided.
        key_type  : Key type — "USER_KEY" (default) or "APP_KEY".

    Returns:
        celonis   : Authenticated Celonis client object.

    Raises:
        EnvironmentError  : If BASE_URL or API_TOKEN are missing.
        ConnectionError   : If authentication fails.
        Exception         : For any other unexpected errors.
    """

    # Step 1 – Use provided args or fall back to .env
    base_url  = base_url  or os.getenv("BASE_URL")
    api_token = api_token or os.getenv("API_TOKEN")

    # Step 2 – Validate credentials exist
    if not base_url:
        raise EnvironmentError(
            "[ERROR] BASE_URL is missing.\n"
            "  Fix : Add BASE_URL=https://your-tenant.eu-1.celonis.cloud to your .env file\n"
            "        or pass it directly: get_connection(base_url='https://...')"
        )

    if not api_token:
        raise EnvironmentError(
            "[ERROR] API_TOKEN is missing.\n"
            "  Fix : Add API_TOKEN=your_token to your .env file\n"
            "        or pass it directly: get_connection(api_token='your_token')"
        )

    # Step 3 – Validate URL format
    if not base_url.startswith("https://"):
        raise ValueError(
            f"[ERROR] BASE_URL looks incorrect: '{base_url}'\n"
            f"  Fix : URL must start with 'https://'\n"
            f"        Example: https://your-tenant.eu-1.celonis.cloud"
        )

    # Step 4 – Attempt connection
    try:
        print(f"  Connecting to : {base_url}")
        celonis = get_celonis(
            base_url=base_url,
            api_token=api_token,
            key_type=key_type,
            permissions=False
        )
        print(f"  Status        : Connected successfully!\n")
        return celonis

    except EnvironmentError as e:
        raise EnvironmentError(
            f"[ERROR] Invalid credentials.\n"
            f"  Reason : {e}\n"
            f"  Fix    : Double-check your BASE_URL and API_TOKEN in .env"
        )

    except ConnectionError as e:
        raise ConnectionError(
            f"[ERROR] Could not reach Celonis.\n"
            f"  Reason : {e}\n"
            f"  Fix    : Check your internet connection or VPN"
        )

    except Exception as e:
        error = str(e).lower()

        if "401" in error or "unauthorized" in error:
            raise PermissionError(
                f"[ERROR] Authentication failed (401 Unauthorized).\n"
                f"  Reason : API token is invalid or expired.\n"
                f"  Fix    : Generate a new API token in Celonis EMS → Edit Profile → API Keys"
            )

        elif "403" in error or "forbidden" in error:
            raise PermissionError(
                f"[ERROR] Access forbidden (403).\n"
                f"  Reason : Your token does not have permission to access this resource.\n"
                f"  Fix    : Ask your Celonis admin to grant the required permissions"
            )

        elif "404" in error or "not found" in error:
            raise ValueError(
                f"[ERROR] Tenant not found (404).\n"
                f"  Reason : BASE_URL may be incorrect.\n"
                f"  Fix    : Check your tenant URL in Celonis EMS"
            )

        elif "timeout" in error:
            raise TimeoutError(
                f"[ERROR] Connection timed out.\n"
                f"  Reason : Celonis EMS did not respond in time.\n"
                f"  Fix    : Check your internet connection or try again later"
            )

        else:
            raise Exception(
                f"[ERROR] Unexpected error during connection.\n"
                f"  Reason : {e}\n"
                f"  Fix    : Check your .env file and try again"
            )


def test_connection():
    """
    Standalone test — connects and prints basic info about
    the first 10 Data Pools and their Models.
    """
    print("=" * 50)
    print("Testing Celonis Connection")
    print("=" * 50 + "\n")

    try:
        celonis = get_connection()

        # Fetch only first 10 pools
        pools = celonis.data_integration.get_data_pools()[:10]
        print(f"  Showing first 10 Data Pool(s):\n")
        for pool in pools:
            print(f"  Pool: {pool.name}")
            models = pool.get_data_models()
            for model in models:
                print(f"    └─ Model: {model.name}")
            print()

        print("=" * 50)
        print("Connection test passed!")
        print("=" * 50)

    except (EnvironmentError, ConnectionError, PermissionError,
            ValueError, TimeoutError, Exception) as e:
        print(f"\n{e}")
        print("\n" + "=" * 50)
        print("Connection test failed!")
        print("=" * 50)


# -----------------------------------------------------------------------
# Run standalone to test connection
# -----------------------------------------------------------------------
if __name__ == "__main__":
    test_connection()
    
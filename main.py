import os
from dotenv import load_dotenv
from pycelonis import get_celonis

# Load the variables from the .env file
load_dotenv()

# Retrieve credentials from environment variables
BASE_URL = os.getenv("CELONIS_URL")
API_TOKEN = os.getenv("CELONIS_API_TOKEN")

def main():
    try:
        print(f"Connecting to {BASE_URL}...")
        celonis = get_celonis(
            base_url=BASE_URL, 
            api_token=API_TOKEN, 
            key_type="USER_KEY"
        )
        
        # 1. Fetch all Data Pools first
        pools = celonis.data_integration.get_data_pools()
        
        print("\n--- Connection Successful! ---")
        print(f"Searching through {len(pools)} Data Pools for models...\n")
        
        # 2. Iterate through each pool to find its models
        model_count = 0
        for pool in pools:
            models = pool.get_data_models()
            for dm in models:
                print(f" - [Pool: {pool.name}] -> Model: {dm.name}")
                model_count += 1
        
        if model_count == 0:
            print("No data models found. Ensure your API token has 'View' permissions on the Data Pools.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main()
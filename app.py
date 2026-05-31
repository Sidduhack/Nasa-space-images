import os
import time
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import requests
from supabase import create_client, Client

# 1. Supabase Connection Configuration
SUPABASE_URL = "https://rjhutdkynphyvsdbzart.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJqaHV0ZGt5bnBoeXZzZGJ6YXJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc4NTY3MjksImV4cCI6MjA5MzQzMjcyOX0.4mks3csRQQ4N2IJ33wz8XdoBJw8RcXlN80G1qXAkOPE"
BUCKET_NAME = "nasa-images"  # Must match your bucket name exactly

# Initialize direct Supabase connection
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

QUERIES = [
    "galaxy", "nebula", "planet", "mars",
    "jupiter", "saturn", "earth from space",
    "moon surface", "deep space", "universe"
]

SPACE_POSITIVE = [
    "galaxy", "nebula", "planet", "mars", "earth", "moon",
    "saturn", "jupiter", "universe", "cosmos", "astronomy",
    "iss", "hubble", "telescope", "star", "black hole"
]

SPACE_NEGATIVE = [
    "people", "celebration", "event", "crew", "meeting",
    "conference", "award", "portrait", "team", "group"
]

visited_ids = set()


def is_space_related(data):
    """Filters metadata using your custom word-matching logic."""
    title = data.get("title", "").lower()
    keywords = " ".join(data.get("keywords", [])).lower()
    text = title + " " + keywords

    if not any(word in text for word in SPACE_POSITIVE):
        return False

    if any(word in text for word in SPACE_NEGATIVE):
        return False

    return True


def get_next_link(data):
    """Extracts next page links strictly matching the official documentation."""
    for link in data.get("collection", {}).get("links", []):
        if link.get("rel") == "next":
            return link["href"]
    return None


def upload_image_to_supabase(url, filename):
    """Streams image bytes directly from NASA's CDN to Supabase Storage."""
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        img_bytes = BytesIO(response.content)
        
        content_type = "image/png" if filename.lower().endswith(".png") else "image/jpeg"
        
        supabase.storage.from_(BUCKET_NAME).upload(
            path=filename,
            file=img_bytes.getvalue(),
            file_options={"content-type": content_type}
        )
        print("✔ Successfully Uploaded to Supabase:", filename)
    except Exception as e:
        print(f"❌ Failed to process or upload {filename}: {e}")


def process_item(data):
    """Fetches the official asset manifest mapping matching your exact child file layout."""
    nasa_id = data.get("nasa_id")

    if not nasa_id or nasa_id in visited_ids:
        return
    visited_ids.add(nasa_id)

    if not is_space_related(data):
        return

    asset_url = f"https://nasa.gov{nasa_id}"
    try:
        res = requests.get(asset_url, timeout=15)
        if res.status_code != 200:
            return
        asset_data = res.json()
        
        items = asset_data.get("collection", {}).get("items", [])
        for asset in items:
            href = asset.get("href", "")

            if "~orig" in href:
                filename = href.split("/")[-1]
                filename = "".join(c for c in filename if c.isalnum() or c in "._-~")
                upload_image_to_supabase(href, filename)
                break
    except Exception as e:
        print(f"⚠️ Error resolving asset mapping index for ID {nasa_id}: {e}")


def main():
    print("🚀 INITIALIZING NASA ORIGINAL PIPELINE ENGINE ON RAILWAY STACK...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:  
        for query in QUERIES:
            print(f"\n🚀 Searching: {query}")
            url = f"https://nasa.gov{query}&media_type=image"

            while url:
                print("Processing:", url)
                try:
                    res_raw = requests.get(url, timeout=15)
                    if res_raw.status_code != 200:
                        print(f"❌ Server rejection during loop step. HTTP Status: {res_raw.status_code}")
                        break
                        
                    res = res_raw.json()
                    items = res.get("collection", {}).get("items", [])

                    for item in items:
                        data_list = item.get("data", [])
                        if data_list:
                            # Pass the inner dictionary down to your original filter logic
                            executor.submit(process_item, data_list[0])

                    url = get_next_link(res)
                    time.sleep(0.3)  # Respectful backoff interval
                    
                except Exception as loop_err:
                    print(f"Encountered collection processing boundary error: {loop_err}")
                    break

if __name__ == "__main__":
    main()
  

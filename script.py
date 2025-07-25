import pandas as pd
import random
import time
from datetime import datetime
from curl_cffi import requests


def read_input_files():
    try:
        loc_df = pd.read_csv("blinkit_locations.csv")
        cat_df = pd.read_csv("blinkit_categories.csv")
        schema_fields = pd.read_csv("Scraping Task _ Schema - Schema.csv", skiprows=1)
        return loc_df, cat_df, schema_fields['Field'].tolist()
    except Exception as err:
        print(f"❌ Error reading input files: {err}")
        return None, None, None


def build_headers(latitude, longitude, l1_name, l1_code, l2_code):
    return {
        'authority': 'blinkit.com',
        'origin': 'https://blinkit.com',
        'referer': f"https://blinkit.com/cn/{l1_name.lower().replace(' ', '-')}/cid/{l1_code}/{l2_code}",
        'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
        'lat': str(latitude),
        'lon': str(longitude),
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }


def collect_data():
    locations, categories, output_columns = read_input_files()
    if not all([locations is not None, categories is not None, output_columns is not None]):
        return

    print("🚀 Scraping started...\n")
    sess = requests.Session(impersonate="chrome120", timeout=30)
    results = []
    today_date = datetime.now().strftime('%Y-%m-%d')

    for _, loc in locations.iterrows():
        lat, lon = loc['latitude'], loc['longitude']
        print(f"📍 Scraping for location: ({lat}, {lon})")

        for _, row in categories.iterrows():
            main_cat, main_id = row['l1_category'], row['l1_category_id']
            sub_cat, sub_id = row['l2_category'], row['l2_category_id']

            print(f"   🔎 Scraping category: {main_cat} > {sub_cat}")

            headers = build_headers(lat, lon, main_cat, main_id, sub_id)
            query = {'l0_cat': main_id, 'l1_cat': sub_id}

            for attempt in range(3):
                try:
                    response = sess.post(
                        url="https://blinkit.com/v1/layout/listing_widgets",
                        headers=headers,
                        params=query,
                        json={}
                    )

                    if response.status_code == 429:
                        print("   ⏳ Hit rate limit. Retrying after a pause...")
                        time.sleep(60)
                        continue

                    if response.status_code == 403:
                        raise requests.errors.HTTPError(response)

                    response.raise_for_status()
                    json_data = response.json()
                    items = json_data.get("response", {}).get("snippets", [])

                    if not items:
                        print("   ⚠️ No product data found.")
                        break

                    print(f"   ✅ Found {len(items)} products.")

                    for entry in items:
                        info = entry.get("data", {})
                        cart = info.get("atc_action", {}).get("add_to_cart", {}).get("cart_item", {})
                        tag = info.get("tracking", {}).get("common_attributes", {})

                        results.append({
                            'date': today_date,
                            'l1_category': main_cat,
                            'l1_category_id': main_id,
                            'l2_category': sub_cat,
                            'l2_category_id': sub_id,
                            'store_id': cart.get('merchant_id'),
                            'variant_id': cart.get('product_id'),
                            'variant_name': cart.get('display_name'),
                            'group_id': cart.get('group_id'),
                            'selling_price': cart.get('price'),
                            'mrp': cart.get('mrp'),
                            'in_stock': not info.get('is_sold_out', True),
                            'inventory': cart.get('inventory'),
                            'is_sponsored': tag.get('badge') == 'AD',
                            'image_url': cart.get('image_url'),
                            'brand': cart.get('brand'),
                            'brand_id': None
                        })

                    break

                except requests.errors.HTTPError as e:
                    print(f"[ERROR] HTTP error: {e}")
                    break
                except Exception as e:
                    print(f"[ERROR] Unexpected error: {e}")
                    break

            time.sleep(random.uniform(1.0, 3.0))

    final_df = pd.DataFrame(results)
    if not final_df.empty:
        final_df = final_df.reindex(columns=output_columns)
        final_df.to_csv("blinkit_scraped_output.csv", index=False)
        print(f"\n✅ Scraping complete. Total records: {len(final_df)}")
    else:
        print("\n⚠️ No data was scraped.")


if __name__ == "__main__":
    print("⏳ Initializing scraper...")
    collect_data()


import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
import time
import os


INPUT_XLSX = r"C:\Users\Zander.Wepener\PyCharmMiscProject\Master_Name_Only-Inno.xlsx"
INPUT_SHEET = 0  
NAME_COL = "Name"  
OUTPUT_CSV = "geocoded_output.csv"
CHECKPOINT_CSV = "geocode_checkpoint.csv"  


MIN_DELAY = 2.0  
BATCH_PAUSE_EVERY = 100  
BATCH_PAUSE_DURATION = 60  
RATE_LIMIT_WAIT = 120  


if not os.path.exists(INPUT_XLSX):
    print(f"ERROR: Cannot find file: {INPUT_XLSX}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Files in current directory: {os.listdir('.')}")
    raise SystemExit("Please update INPUT_XLSX with the correct path to your Excel file.")


print(f"Reading {INPUT_XLSX}...")
df = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET, dtype=str)
if NAME_COL not in df.columns:
    raise SystemExit(f"Column '{NAME_COL}' not found. Columns: {list(df.columns)}")

names = df[NAME_COL].fillna("").astype(str).tolist()
print(f"Found {len(names)} names to geocode.")


user_agent = "YourAppName_GoGlobal_geocode_v1 (contact: your-email@example.com)"
geolocator = Nominatim(user_agent=user_agent, timeout=10)


results = {}
if os.path.exists(CHECKPOINT_CSV):
    print(f"Loading checkpoint from {CHECKPOINT_CSV}...")
    chk = pd.read_csv(CHECKPOINT_CSV, dtype=str)
    for _, r in chk.iterrows():
        results[r["original_name"]] = r.to_dict()
    print(f"Loaded {len(results)} previously geocoded entries.")


out_rows = []
total = len(names)
requests_made = 0
last_request_time = 0

for i, original_name in enumerate(names, start=1):
    if original_name in results:
        out_rows.append(results[original_name])
        if i % 100 == 0:
            print(f"Progress: {i}/{total} ({i * 100 // total}%) - Skipped (cached)")
        continue

    row = {
        "original_name": original_name,
        "query": original_name,
        "display_name": "",
        "latitude": "",
        "longitude": "",
        "address": "",
        "raw": ""
    }

    if not original_name.strip():
        out_rows.append(row)
        results[original_name] = row
        continue

    
    time_since_last = time.time() - last_request_time
    if time_since_last < MIN_DELAY:
        time.sleep(MIN_DELAY - time_since_last)

    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(original_name)
            last_request_time = time.time()

            
            if location:
                try:
                    row["display_name"] = str(location.address) if location.address else ""
                    row["latitude"] = str(location.latitude) if hasattr(location, 'latitude') else ""
                    row["longitude"] = str(location.longitude) if hasattr(location, 'longitude') else ""
                    row["address"] = str(location.address) if location.address else ""
                    row["raw"] = str(location.raw) if hasattr(location, 'raw') else ""
                except Exception as e:
                    
                    print(f"⚠ Could not parse location for '{original_name}': {e}")

            requests_made += 1
            break  

        except Exception as e:
            error_str = str(e)

            
            if "403" in error_str or "Forbidden" in error_str or "429" in error_str:
                print(f"⚠ Rate limited on '{original_name}' (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"   ⏸ Waiting {RATE_LIMIT_WAIT} seconds before retry...")
                    time.sleep(RATE_LIMIT_WAIT)
                else:
                    print(f"   ✗ Max retries reached - skipping this record")
                    break
            else:
                
                print(f"⚠ Error for '{original_name}': {type(e).__name__}")
                break

    
    out_rows.append(row)
    results[original_name] = row

    
    if requests_made > 0 and requests_made % BATCH_PAUSE_EVERY == 0:
        print(f"\n⏸ Processed {requests_made} requests. Pausing for {BATCH_PAUSE_DURATION}s to avoid rate limits...")
        time.sleep(BATCH_PAUSE_DURATION)

    
    if i % 10 == 0:
        print(f"Progress: {i}/{total} ({i * 100 // total}%)")

    
    if i % 50 == 0:
        pd.DataFrame(list(results.values())).to_csv(CHECKPOINT_CSV, index=False)
        print(f"Checkpoint saved at row {i}")


pd.DataFrame(out_rows).to_csv(OUTPUT_CSV, index=False)
pd.DataFrame(list(results.values())).to_csv(CHECKPOINT_CSV, index=False)

print(f"\n{'=' * 60}")
print("GEOCODING SUMMARY")
print('=' * 60)
df_summary = pd.DataFrame(out_rows)
total_processed = len(df_summary)
successful = ((df_summary['latitude'] != '') & (df_summary['latitude'].notna())).sum()
blank = ((df_summary['latitude'] == '') | (df_summary['latitude'].isna())).sum()

print(f"Total processed: {total_processed}")
print(f"✓ Successfully geocoded: {successful}")
print(f"○ Blank/Not found: {blank}")
print(f"\n📄 Results saved to: {OUTPUT_CSV}")
print('=' * 60)

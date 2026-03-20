# ============================================================
# Enquiry Data Pipeline
# ============================================================
# Author  : Sivaram Vishnu K
# Role    : Data Analyst
# GitHub  : github.com/sivaramvishnuk
# LinkedIn: linkedin.com/in/sivaramvishnuk
#
# Description:
#   Automated pipeline to process raw marketing enquiry CSV/Excel
#   files into a clean, analysis-ready manipulated dataset.
#
# What it does:
#   - Maps city, source, and service columns (case-insensitive)
#   - Filters Performance and App sources only
#   - Removes unserviceable cities (N/A)
#   - Deduplicates by mobile number
#   - Applies Source_UTM_2 replacement rules
#   - Formats date column as dd/mm/yyyy
#   - Saves output as "March Mtd Enq Data_DD.csv / .xlsx"
#
# Usage:
#   python enq_manipulate.py
#   python enq_manipulate.py "path/to/your/raw_file.csv"
#
# Requirements:
#   pip install pandas openpyxl
# ============================================================

import pandas as pd
import sys
import os

# ============================================================
# FILE PATH — change this to your new raw file each time
# ============================================================
if len(sys.argv) > 1:
    file_path = sys.argv[1]
else:
    file_path = r"C:\Users\YourName\Downloads\your_raw_file.csv"

# ============================================================
# STEP 1: Load raw file (CSV or Excel)
# ============================================================
print("=" * 60)
print("STEP 1: Loading raw file...")
print("=" * 60)

lower = file_path.lower()
if lower.endswith('.csv'):
    df = pd.read_csv(file_path)
elif lower.endswith(('.xls', '.xlsx')):
    df = pd.read_excel(file_path, sheet_name='raw')
else:
    raise ValueError(f"Unsupported file type: {file_path}")

print(f"  Raw data loaded: {len(df)} rows x {len(df.columns)} columns")

# ============================================================
# STEP 2: Hardcoded Sheet2 lookup maps
#         ALL keys stored in LOWERCASE → case-insensitive matching
#         noida / Noida / NOIDA → all map correctly
# ============================================================
print("\n" + "=" * 60)
print("STEP 2: Loading hardcoded Sheet2 mappings (case-insensitive)...")
print("=" * 60)

def ci(d):
    """Convert all dict keys to lowercase for case-insensitive lookup."""
    return {k.lower(): v for k, v in d.items()}

def ci_get(d_lower, raw_val):
    """Case-insensitive dict lookup."""
    if pd.isna(raw_val):
        return pd.NA
    return d_lower.get(str(raw_val).strip().lower(), pd.NA)

# SOURCE MAP
source_map = ci({
    'App Dent'                     : 'Performance',
    'On-Call Bookings'             : 'Performance',
    'Core-Effort'                  : 'Performance',
    'Dent Promo'                   : 'Performance',
    'External Bookings'            : 'Performance',
    'Facebook'                     : 'Performance',
    'GMB'                          : 'GMB',
    'GoBumpr App'                  : 'Performance',
    'Google'                       : 'Performance',
    'Hub Booking'                  : 'Performance',
    'Re-Engagement Bookings'       : 'Outbound marketing',
    'myTVS app'                    : 'App',
    'myTVS web'                    : 'Performance',
    'promo'                        : 'Performance',
    'Facebook webhook'             : 'Performance',
    'Summer_Campaign'              : 'Performance',
    'Whatsapp RE'                  : 'Outbound marketing',
    'myTVS Toll Free'              : 'myTVS Toll Free',
    'myTVS_app_query'              : 'App',
    'mytvs_web'                    : 'Performance',
    'Google video ads'             : 'Performance',
    'L1 & L2 Bookings'             : 'Performance',
    'Cars24'                       : 'Cars24',
    'MyTVS Whatsapp'               : 'Outbound marketing',
    'Smartoutlet'                  : 'Smartoutlet',
    'RE-Probing'                   : 'Performance',
    'promo-OTP_Dropout'            : 'Performance',
    'Whatsapp LP'                  : 'Outbound marketing',
    'Gobumpr_App-Enquiry'          : 'Performance',
    'shriram_insurance'            : 'Performance',
    'FIT_PMS'                      : 'Smart Outlet',
    'website'                      : 'Performance',
    'TVS Win-Back'                 : 'Performance',
    'promo-Make_and_model_Verified': 'Performance',
    'RE-Effort'                    : 'Performance',
    'SO_FIT'                       : 'Smart Outlet',
    'BTL Bookings'                 : 'BTL',
    'Google_API'                   : 'Performance',
    'WhatsApp'                     : 'Outbound marketing',
    'Elision_On_Call'              : 'GMB',
})

# CITY3 MAP (city_new -> state code)
city3_map = ci({
    'Chennai'           : 'TN',  'Bangalore'          : 'KA',
    'Hyderabad'         : 'TL',  'Coimbatore'         : 'TN',
    'Madurai'           : 'TN',  'Salem'              : 'TN',
    'Mumbai'            : 'NW',  'Thane'              : 'NW',
    'Jaipur'            : 'NW',  'Pune'               : 'NW',
    'Ahmedabad'         : 'NW',  'Kerala'             : 'KL',
    'Kollam'            : 'KL',  'Ernakulam'          : 'KL',
    'Trivandrum'        : 'KL',  'Kochi'              : 'KL',
    'Kattappana'        : 'KL',  'Alappuzha'          : 'KL',
    'Thiruvananthapuram': 'KL',  'Noida'              : 'NW',
    'Gurgaon'           : 'NW',  'Gurugram'           : 'NW',
    'Bengaluru'         : 'KA',  'Indore'             : 'NW',
    'Gandhinagar'       : 'NW',  'Kattapana'          : 'KL',
    'Delhi'             : 'NW',  'Vishakhapatnam'     : 'AP',
    'Trichy'            : 'TN',  'Visakhapatnam'      : 'AP',
    'Bhopal'            : 'NW',  'Faridabad'          : 'NW',
    'Mysore'            : 'KA',  'New Delhi'          : 'NW',
    'Vijayawada'        : 'AP',  'Vizag'              : 'AP',
    'Tiruchirappalli'   : 'TN',  'Udupi'              : 'KA',
    'Mysure'            : 'KA',  'Vadodara'           : 'NW',
    'Mangaluru'         : 'KA',  'Mangalore'          : 'KA',
    'Warangal'          : 'TL',  'Hosur'              : 'TN',
    'Idukki'            : 'KL',  'Gandhi_nagar'       : 'NW',
    'Thrissur'          : 'KL',  'Lucknow'            : 'NW',
    'Nellore'           : 'AP',  'Surat'              : 'NW',
})

# CITY2 MAP (city_new -> normalized city name)
city2_map = ci({
    'Chennai'           : 'Chennai',    'Bangalore'          : 'Bangalore',
    'Hyderabad'         : 'Hyderabad',  'Coimbatore'         : 'Coimbatore',
    'Madurai'           : 'Madurai',    'Salem'              : 'Salem',
    'Mumbai'            : 'Mumbai',     'Thane'              : 'Mumbai',
    'Jaipur'            : 'Jaipur',     'Pune'               : 'Pune',
    'Ahmedabad'         : 'Ahmedabad',  'Kollam'             : 'Kerala',
    'Ernakulam'         : 'Kerala',     'Trivandrum'         : 'Kerala',
    'Kochi'             : 'Kerala',     'Kerala'             : 'Kerala',
    'Kattappana'        : 'Kerala',     'Alappuzha'          : 'Kerala',
    'Thiruvananthapuram': 'Kerala',     'Noida'              : 'Noida',
    'Gurgaon'           : 'Gurugram',   'Gurugram'           : 'Gurugram',
    'Bengaluru'         : 'Bangalore',  'Indore'             : 'Indore',
    'Gandhinagar'       : 'Ahmedabad',  'Kattapana'          : 'Kerala',
    'Delhi'             : 'Delhi',      'Vishakhapatnam'     : 'Vizag',
    'Trichy'            : 'Trichy',     'Visakhapatnam'      : 'Vizag',
    'Bhopal'            : 'Bhopal',     'Faridabad'          : 'Faridabad',
    'Mysore'            : 'Mysore',     'New Delhi'          : 'New Delhi',
    'Udupi'             : 'Udupi',      'Vijayawada'         : 'Vijayawada',
    'Vizag'             : 'Vizag',      'Tiruchirappalli'    : 'Trichy',
    'Mysure'            : 'Mysore',     'Vadodara'           : 'Vadodara',
    'Mangaluru'         : 'Mangalore',  'Mangalore'          : 'Mangalore',
    'Warangal'          : 'Warangal',   'Hosur'              : 'Hosur',
    'Idukki'            : 'Kerala',     'Gandhi_nagar'       : 'Ahmedabad',
    'Thrissur'          : 'Kerala',     'Lucknow'            : 'Lucknow',
    'Nellore'           : 'Nellore',    'Surat'              : 'Surat',
})

# PRO MAP
pro_map = ci({
    'Car AC Service'                : 'AC',
    'Car Bumper Repainting'         : 'Detailing',
    'Car Express Service'           : 'Exp',
    'Car Interior Detailing'        : 'Detailing',
    'Car Machine Polish'            : 'Body Works',
    'Car Repair & Service'          : 'Repair',
    'Car Repair Job'                : 'Repair',
    'Car Repair service'            : 'Repair',
    'Car Underchassis Rust Coating' : 'Body Works',
    'Car dent / scratch removal'    : 'Dent',
    'Dent and Scratch Removal'      : 'Dent',
    'Periodic Maintenance Service'  : 'PMS',
    'Periodic_maintenance_'         : 'PMS',
    'Full Body Painting'            : 'Body Works',
    'Car AC Service and Repair'     : 'AC',
    'AC Service 999'                : 'AC',
    'AC Service and Repair'         : 'AC',
    'Bumper Repainting 2999'        : 'Bumper Repaint',
    'Rear Bumper Paint'             : 'Bumper Repaint',
    'Front Bumper Paint'            : 'Bumper Repaint',
    'Others'                        : 'Others',
})

# PRO2 MAP
pro2_map = ci({
    'Car AC Service'                : 'AC',
    'Car Bumper Repainting'         : 'Bumper Repaint',
    'Car Express Service'           : 'Exp',
    'Car Interior Detailing'        : 'Detailing',
    'Car Machine Polish'            : 'Body Works',
    'Car Repair & Service'          : 'Repair',
    'Car Repair Job'                : 'Repair',
    'Car Repair service'            : 'Repair',
    'Car Underchassis Rust Coating' : 'Body Works',
    'Car dent / scratch removal'    : 'Dent',
    'Dent and Scratch Removal'      : 'Dent',
    'Periodic Maintenance Service'  : 'PMS',
    'Periodic_maintenance_'         : 'PMS',
    'Full Body Painting'            : 'Body Works',
    'Car AC Service and Repair'     : 'AC',
    'AC Service 999'                : 'AC',
    'AC Service and Repair'         : 'AC',
    'Bumper Repainting 2999'        : 'Bumper Repaint',
    'Rear Bumper Paint'             : 'Bumper Repaint',
    'Front Bumper Paint'            : 'Bumper Repaint',
    'Others'                        : 'Others',
})

# NAME2 SET (case-insensitive)
name2_set_lower = {n.lower() for n in {
    # Add your agent names here (agents who handle core enquiries)
    # Example: 'Agent Name One', 'Agent Name Two',
}}

# LOOKUP MAP (case-insensitive)
lookup_map = ci({
    # Add your agent name to team mappings here
    # Format: 'Agent Full Name': 'CORE',  (or 'SO', 'JD', 'BTL')
    # Example:
    # 'John Smith'  : 'CORE',
    # 'Jane Doe'    : 'SO',
    # 'Bob Johnson' : 'JD',
})

print(f"  Source map : {len(source_map)} entries")
print(f"  City3 map  : {len(city3_map)} entries")
print(f"  City2 map  : {len(city2_map)} entries")
print(f"  PRO map    : {len(pro_map)} entries")
print(f"  PRO2 map   : {len(pro2_map)} entries")
print(f"  Lookup map : {len(lookup_map)} entries")
print(f"  NOTE: All maps are CASE-INSENSITIVE")
print(f"        noida / Noida / NOIDA → all map to 'Noida'")

# ============================================================
# STEP 3: Add mapped columns (case-insensitive)
# ============================================================
print("\n" + "=" * 60)
print("STEP 3: Adding mapped columns (case-insensitive)...")
print("=" * 60)

df['CITY3'] = df['city_new'].apply(lambda x: ci_get(city3_map, x))
df['CITY2'] = df['city_new'].apply(lambda x: ci_get(city2_map, x))
df['PRO']   = df['master_service'].apply(lambda x: ci_get(pro_map, x))
df['PRO2']  = df['master_service'].apply(lambda x: ci_get(pro2_map, x))
df['MAP']   = df['source'].apply(lambda x: ci_get(source_map, x))

agent_col = 'Agent_Name' if 'Agent_Name' in df.columns else 'name'

df['NAME2'] = df[agent_col].apply(
    lambda x: x if pd.notna(x) and str(x).strip().lower() in name2_set_lower else pd.NA
)
df['LOOKUP'] = df[agent_col].apply(lambda x: ci_get(lookup_map, x))

print(f"  MAP   — mapped: {df['MAP'].notna().sum()}, unmapped: {df['MAP'].isna().sum()}")
print(f"  CITY2 — mapped: {df['CITY2'].notna().sum()}, unmapped: {df['CITY2'].isna().sum()}")
print(f"  PRO   — mapped: {df['PRO'].notna().sum()}, unmapped: {df['PRO'].isna().sum()}")

# ============================================================
# STEP 4: Filter — keep only Performance and App
# ============================================================
print("\n" + "=" * 60)
print("STEP 4: Filtering Performance and App rows...")
print("=" * 60)

before_filter = len(df)
df = df[df['MAP'].isin(['Performance', 'App'])].copy()
after_filter = len(df)
print(f"  Before : {before_filter} rows  |  After : {after_filter} rows  |  Removed : {before_filter - after_filter}")

# ============================================================
# STEP 5: Keep ONLY Performance and App — remove all other sources
# ============================================================
print("\n" + "=" * 60)
print("STEP 5: Keeping only Performance and App rows...")
print("=" * 60)

before_filter = len(df)
df = df[df['MAP'].isin(['Performance', 'App'])].copy()
print(f"  Before : {before_filter} rows")
print(f"  After  : {len(df)} rows (Performance + App only)")
print(f"  Removed: {before_filter - len(df)} (other sources)")

# ============================================================
# STEP 6: Handle unmatched CITY2 — remove N/A (unserviceable cities)
#         This must happen BEFORE deduplication
# ============================================================
print("\n" + "=" * 60)
print("STEP 6: Removing unserviceable cities (N/A) first...")
print("=" * 60)

service_in_city = {
    'periodic_maintenance_', 'full_body_painting', 'car_repair_service',
    'car_ac_service', 'car_express_service', 'car_interior_detailing',
    'car_machine_polish', 'car_repair_job', 'car_bumper_repainting',
    'car_underchassis_rust_coating', 'dent_and_scratch_removal',
    'car_repair_&_service', 'complete_car_detailing', 'complete_car_spa',
    'car_wash_&_wax', 'mileage+', 'monsoon_protect', 'cars24', 'others',
    'bumper_repainting_2999', 'rear_bumper_paint', 'front_bumper_paint',
    'ac_service_999',
    'periodic maintenance service', 'full body painting',
    'car repair service', 'car ac service', 'car express service',
    'car interior detailing', 'car machine polish', 'car repair job',
    'car bumper repainting', 'car underchassis rust coating',
    'dent and scratch removal', 'car repair & service',
    'complete car detailing', 'complete car spa', 'car wash & wax',
    'bumper repainting 2999', 'rear bumper paint', 'front bumper paint',
    'ac service 999', 'mileage+', 'monsoon protect',
}

def fix_city2(row):
    city2    = row['CITY2']
    city_raw = str(row['city_new']).strip().lower() if pd.notna(row['city_new']) else ''
    if pd.notna(city2):
        return city2
    if city_raw in service_in_city:
        return 'Others'
    return pd.NA

before_city = len(df)
df['CITY2'] = df.apply(fix_city2, axis=1)
others_count = (df['CITY2'] == 'Others').sum()
df = df[df['CITY2'].notna()].copy().reset_index(drop=True)
after_city = len(df)

print(f"  Before           : {before_city} rows")
print(f"  Marked as Others : {others_count}")
print(f"  Deleted (N/A)    : {before_city - after_city} unserviceable cities")
print(f"  After            : {after_city} rows")

# ============================================================
# STEP 6B: NOW deduplicate by mobile_number
#          Only serviceable city data remains at this point
#          Normalize mobile: strip '+' so +919876543210 == 919876543210
# ============================================================
print("\n" + "=" * 60)
print("STEP 6B: Deduplicating serviceable city data by mobile_number...")
print("=" * 60)

# Normalize mobile number — strip '+' prefix for consistent comparison
df['mobile_norm'] = df['mobile_number'].astype(str).str.replace('+', '', regex=False).str.strip()

before_dedup = len(df)
df = df.drop_duplicates(subset=['mobile_norm'], keep='first').reset_index(drop=True)
after_dedup = len(df)

# Drop the helper column after dedup
df = df.drop(columns=['mobile_norm'])

print(f"  Before dedup : {before_dedup} rows")
print(f"  After dedup  : {after_dedup} rows")
print(f"  Removed      : {before_dedup - after_dedup} duplicates")

# ============================================================
# STEP 7: Reorder columns
# ============================================================
print("\n" + "=" * 60)
print("STEP 7: Reordering columns...")
print("=" * 60)

# Add blank Outskirts column before reordering so col_order places it correctly
if 'Outskirts' not in df.columns:
    df['Outskirts'] = ''
    print("  ✅ 'Outskirts' column created (blank)")

col_order = [
    'Source_UTM_2', 'city_new', 'CITY3', 'CITY2',
    'booking_id', 'Outskirts', 'user_id', 'shop_name',
    'service_type', 'booking_status', 'master_service',
    'PRO', 'PRO2', 'vehicle_type', 'log', 'Date', 'city',
    'b2b_swap_flag', 'source', 'MAP',
    'b2b_vehicle_at_garage', 'b2b_check_in_report',
    'service_status', 'utm_source', 'master_source', 'name',
    'Enq_Type', 'Lead_Type', 'utm_campaign',
    'utm_source Split - 1', 'Source_utm_1',
    'utm_campaign Split - 1', 'Google_Split',
    'utm_adgroup', 'utm_content', 'utm_id', 'utm_term',
    'url', 'gcl_id', 'Vehicle type', 'Check-in_flag',
    'Check', 'locality', 'mobile_number', 'utm_medium',
    'trim_service', 'service_type1', 'service_type2',
    'live_flag', 'jd_flag', 'axle_flag', 'flag',
    'Source_Url', 'Agent_Name', 'NAME2', 'pick_up',
    'user_veh_id', 'service_description',
    'flag_unwntd', 'flag_duplicate', 'LOOKUP', 'locality_web'
]

existing = [c for c in col_order if c in df.columns]
extras   = [c for c in df.columns if c not in col_order]
df = df[existing + extras]

print(f"  Final columns : {len(df.columns)}")
print(f"  Final rows    : {len(df)}")




# ============================================================
# STEP 7B: Source_UTM_2 (col A) replacement rules
# ============================================================
print("\n" + "=" * 60)
print("STEP 7B: Applying Source_UTM_2 replacement rules...")
print("=" * 60)

col_A   = 'Source_UTM_2'   # Column A
col_AL  = 'url'             # Column AL — url
col_S   = 'source'          # Column S  — source
col_utm = 'utm_medium'      # utm_medium column

def col_contains(series, *keywords):
    """Case-insensitive contains check for any keyword."""
    mask = pd.Series([False] * len(series), index=series.index)
    for kw in keywords:
        mask = mask | series.fillna('').str.lower().str.contains(kw.lower(), na=False)
    return mask

# ---- RULE 1: Promo Others in col A ----
# Priority order:
#   1. col X (utm_source) has facebook keywords (ig, fb, facebook, fb-sitelink etc.) → Facebook
#   2. col X (utm_source) has google keywords (google, search)                        → Google
#   3. col AL (url) has fb/facebook                                                   → Facebook
#   4. col AL (url) has search/google                                                 → Google
#   5. Remaining → split equally Facebook & Google

col_X = 'utm_source'   # Column X

# Facebook related keywords in utm_source
FB_KEYWORDS  = ['ig', 'fb', 'facebook', 'fb-sitelink', 'instagram',
                 'fb_', '_fb', 'fb-', '-fb', 'fbclid']
# Google related keywords in utm_source
GGL_KEYWORDS = ['google', 'search', 'gdn', 'pmax', 'gsearch', 'gdisplay']

mask_promo = df[col_A].fillna('').str.strip().str.lower() == 'promo others'
if mask_promo.sum() > 0:

    # Step 1: Check col X (utm_source) for Facebook keywords
    mask_utm_fb  = mask_promo & col_contains(df[col_X], *FB_KEYWORDS)

    # Step 2: Check col X (utm_source) for Google keywords (not already fb)
    mask_utm_ggl = mask_promo & ~mask_utm_fb & col_contains(df[col_X], *GGL_KEYWORDS)

    # Step 3: Remaining after utm_source check → check col AL (url)
    mask_remaining = mask_promo & ~mask_utm_fb & ~mask_utm_ggl
    mask_url_fb    = mask_remaining & col_contains(df[col_AL], 'fb', 'facebook')
    mask_url_ggl   = mask_remaining & ~mask_url_fb & col_contains(df[col_AL], 'search', 'google')

    # Step 4: Still remaining → split equally
    mask_neither   = mask_remaining & ~mask_url_fb & ~mask_url_ggl

    # Apply replacements
    df.loc[mask_utm_fb,  col_A] = 'Facebook'
    df.loc[mask_utm_ggl, col_A] = 'Google'
    df.loc[mask_url_fb,  col_A] = 'Facebook'
    df.loc[mask_url_ggl, col_A] = 'Google'

    # Split remaining equally
    neither_idx = df[mask_neither].index.tolist()
    half = len(neither_idx) // 2
    df.loc[neither_idx[:half],  col_A] = 'Facebook'
    df.loc[neither_idx[half:],  col_A] = 'Google'

    print(f"  Promo Others total         : {mask_promo.sum()}")
    print(f"  → Facebook (utm_source X)  : {mask_utm_fb.sum()}")
    print(f"  → Google   (utm_source X)  : {mask_utm_ggl.sum()}")
    print(f"  → Facebook (url AL)        : {mask_url_fb.sum()}")
    print(f"  → Google   (url AL)        : {mask_url_ggl.sum()}")
    print(f"  → Split equally (neither)  : {len(neither_idx)} ({half} FB / {len(neither_idx)-half} Google)")
else:
    print("  No 'Promo Others' rows — skipping Rule 1")

# ---- RULE 2: Google Others in col A → Google ----
mask_ggl_others = df[col_A].fillna('').str.strip().str.lower() == 'google others'
df.loc[mask_ggl_others, col_A] = 'Google'
print(f"\n  Google Others → Google : {mask_ggl_others.sum()} rows")

# ---- RULE 3: Organic in col A ----
# col S (source) has 'Facebook webhook' → col A = Facebook
# col S (source) has 'Google_API'       → col A = Google
mask_organic = df[col_A].fillna('').str.strip().str.lower() == 'organic'
if mask_organic.sum() > 0:
    mask_org_fb  = mask_organic & col_contains(df[col_S], 'facebook webhook')
    mask_org_ggl = mask_organic & col_contains(df[col_S], 'google_api')
    df.loc[mask_org_fb,  col_A] = 'Facebook'
    df.loc[mask_org_ggl, col_A] = 'Google'
    print(f"\n  Organic total         : {mask_organic.sum()}")
    print(f"  → Facebook            : {mask_org_fb.sum()}")
    print(f"  → Google              : {mask_org_ggl.sum()}")
else:
    print("\n  No 'Organic' rows — skipping Rule 3")

# ---- RULE 4: External Bookings in col A ----
# If utm_medium is blank / has 'missed call' / has 'whatsapp'
# → replace utm_medium column value with 'Whatsapp/ sms'
# → col A stays as 'External Bookings' (UNCHANGED)
mask_ext = df[col_A].fillna('').str.strip().str.lower() == 'external bookings'
if mask_ext.sum() > 0 and col_utm in df.columns:
    mask_blank   = mask_ext & df[col_utm].isna()
    mask_missed  = mask_ext & col_contains(df[col_utm], 'missed call')
    mask_whatsap = mask_ext & col_contains(df[col_utm], 'whatsapp')
    mask_replace = mask_blank | mask_missed | mask_whatsap
    df.loc[mask_replace, col_utm] = 'Whatsapp/ sms'
    print(f"\n  External Bookings total        : {mask_ext.sum()}")
    print(f"  → utm_medium blank             : {mask_blank.sum()}")
    print(f"  → utm_medium has missed call   : {mask_missed.sum()}")
    print(f"  → utm_medium has whatsapp      : {mask_whatsap.sum()}")
    print(f"  → Total utm_medium updated     : {mask_replace.sum()}")
    print(f"  NOTE: col A 'External Bookings' kept UNCHANGED ✅")
else:
    print("\n  No 'External Bookings' rows or utm_medium missing — skipping Rule 4")

# ---- RULE 5: col A contains 'search' → replace with 'Google' ----
mask_search = df[col_A].fillna('').str.strip().str.lower().str.contains('search', na=False)
df.loc[mask_search, col_A] = 'Google'
print(f"\n  Col A contains 'search' → Google : {mask_search.sum()} rows")

print(f"\n  Col A top values after all rules:")
print(df[col_A].value_counts().head(15).to_string())

# ============================================================
# STEP 8: Dynamic filename + Date format → 01-Mar-26
# ============================================================
print("\n" + "=" * 60)
print("STEP 8: Building output filename + formatting Date column...")
print("=" * 60)

date_col = 'Date' if 'Date' in df.columns else 'log'

# Parse to datetime first (to get last date for filename)
date_parsed = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
last_date = date_parsed.max()
date_str = last_date.strftime('%d') if pd.notna(last_date) else 'XX'
print(f"  Last date in data : {last_date}")
print(f"  Date suffix       : {date_str}")

# Save as TEXT string in dd-Mon-yy format → e.g. 01-Mar-26
# Using str ensures Excel treats it as text, not a date serial number
df[date_col] = date_parsed.apply(
    lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else ''
)
print(f"  Date column format: 01/03/2026  ✅")
print(f"  Sample dates      : {df[date_col].dropna().head(3).tolist()}")

out_filename = f"March Mtd Enq Data_{date_str}"

# ============================================================
# STEP 9: Save output
# ============================================================
print("\n" + "=" * 60)
print("STEP 9: Saving output...")
print("=" * 60)

input_dir = os.path.dirname(file_path)

if lower.endswith('.csv'):
    out_path = os.path.join(input_dir, f"{out_filename}.csv")
    df.to_csv(out_path, index=False)
    print(f"  Processed CSV saved : {out_path}")
elif lower.endswith(('.xls', '.xlsx')):
    out_path = os.path.join(input_dir, f"{out_filename}.xlsx")
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='manipulated', index=False)
    print(f"  Excel saved to      : {out_path}")
    print(f"  Sheet name          : manipulated")
else:
    raise ValueError(f"Unsupported file type: {file_path}")

print("\n✅ Done!")

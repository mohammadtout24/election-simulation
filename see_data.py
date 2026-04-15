import pandas as pd
import os
import re

# ==========================================
# 1. SETUP PATHS (Same as your app)
# ==========================================
USER_HOME = os.path.expanduser("~")
PROJECT_DIR = os.path.join(USER_HOME, "Downloads", "election_map1")
PATH_2022 = os.path.join(PROJECT_DIR, 'data')
PATH_2018 = os.path.join(PROJECT_DIR, 'data_2')

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def clean_text(text):
    """Normalize text: remove spaces, lowercase"""
    if not isinstance(text, str): return ""
    return re.sub(r'\s+', ' ', text.strip()).lower()

# ==========================================
# 3. CORE LOGIC (Extracted from get_candidates_df)
# ==========================================
def process_and_print_dataframe(filename):
    # Determine path (2022 vs 2018)
    path_1 = os.path.join(PATH_2022, filename)
    path_2 = os.path.join(PATH_2018, filename)
    
    target_path = None
    if os.path.exists(path_1): target_path = path_1
    elif os.path.exists(path_2): target_path = path_2
    else: 
        print(f"❌ ERROR: File not found: {filename}")
        return None
    
    print(f"✅ LOADING FILE: {target_path}")

    try:
        xl = pd.ExcelFile(target_path, engine='openpyxl')
        
        # --- PROCESS SHEET 1 (Winners) ---
        df_sheet1 = xl.parse(0)
        df_sheet1.columns = [str(c).strip().upper() for c in df_sheet1.columns]
        
        winners_set = set()
        if 'HOLDER' in df_sheet1.columns:
            raw_winners = df_sheet1['HOLDER'].dropna().astype(str)
            winners_set = set(raw_winners.apply(clean_text).tolist())
            print(f"   -> Found {len(winners_set)} winners in Sheet 1.")
        else:
            print(f"   ⚠️ WARNING: 'HOLDER' column MISSING in Sheet 1.")

        # --- PROCESS SHEET 2 (Data) ---
        # Try second sheet, fallback to first if only 1 exists
        sheet_index = 1 if len(xl.sheet_names) > 1 else 0
        df_sheet2 = xl.parse(sheet_index)
        df_sheet2.columns = [str(c).strip().upper() for c in df_sheet2.columns]
        
        # Column Mapping
        col_map = {
            'CANDIDATE': 'MEMBER', 'NAME': 'MEMBER', 'المرشح': 'MEMBER', 'CANDIDATE NAME': 'MEMBER', 
            'HOLDER': 'MEMBER', 'MEMBER': 'MEMBER', 'اسم المرشح': 'MEMBER',
            'LIST': 'GROUP', 'LIST NAME': 'GROUP', 'اللائحة': 'GROUP', 'اسم اللائحة': 'GROUP',
            'VOTES': 'VOTES', 'PREFERENTIAL VOTES': 'VOTES', 'الأصوات': 'VOTES', 
            'الأصوات التفضيلية': 'VOTES', 'اصوات تفضيلية': 'VOTES', 'عدد الأصوات': 'VOTES', 
            'TOTAL VOTES': 'VOTES', 'مجموع الأصوات': 'VOTES', 'VOTE_LIST': 'VOTES',
            'SECT': 'RELIGION', 'CONFESSION': 'RELIGION', 'المذهب': 'RELIGION', 'الطائفة': 'RELIGION',
            'REGION': 'DISTRICT', 'QADA': 'DISTRICT', 'MINOR DISTRICT': 'DISTRICT', 
            'الدائرة الصغرى': 'DISTRICT', 'SUB DISTRICT': 'DISTRICT', 'القضاء': 'DISTRICT'
        }
        df_sheet2 = df_sheet2.rename(columns=col_map)
        
        # Validation
        if 'MEMBER' not in df_sheet2.columns: 
            print("   ❌ ERROR: Could not find Candidate Name column (MEMBER).")
            print(f"   Available columns: {list(df_sheet2.columns)}")
            return None
        
        # Clean Votes
        if 'VOTES' not in df_sheet2.columns:
            print("   ⚠️ WARNING: Could not find Votes column. Setting to 0.")
            df_sheet2['VOTES'] = 0
        else:
            df_sheet2['VOTES'] = pd.to_numeric(df_sheet2['VOTES'], errors='coerce').fillna(0).astype(int)

        # Defaults
        if 'RELIGION' not in df_sheet2.columns: df_sheet2['RELIGION'] = 'Unknown'
        if 'DISTRICT' not in df_sheet2.columns: df_sheet2['DISTRICT'] = 'General'
        if 'GROUP' not in df_sheet2.columns: df_sheet2['GROUP'] = 'Independent'

        # Calculate Winner Boolean
        if winners_set:
            df_sheet2['clean_name'] = df_sheet2['MEMBER'].apply(clean_text)
            df_sheet2['IS_WINNER'] = df_sheet2['clean_name'].isin(winners_set)
        else:
            df_sheet2['IS_WINNER'] = False
        
        # Cleanup temporary column
        if 'clean_name' in df_sheet2.columns:
            del df_sheet2['clean_name']

        return df_sheet2

    except Exception as e: 
        print(f"   ❌ System Error: {e}")
        return None

# ==========================================
# 4. EXECUTION
# ==========================================
if __name__ == "__main__":
    # AUTO-DETECT a file to test, or you can hardcode one
    # Example hardcode: filename_to_test = "Beirut1_result.xlsx"
    
    filename_to_test = None
    
    # Try to find the first Excel file in the 2022 folder
    if os.path.exists(PATH_2022):
        files = [f for f in os.listdir(PATH_2022) if f.endswith('.xlsx')]
        if files:
            filename_to_test = files[0] # Pick the first one found
    
    if filename_to_test:
        print(f"--- Processing: {filename_to_test} ---")
        df = process_and_print_dataframe(filename_to_test)
        
        if df is not None:
            print("\n" + "="*50)
            print("FINAL DATAFRAME INFO")
            print("="*50)
            print(df.info())
            
            print("\n" + "="*50)
            print("FIRST 10 ROWS")
            print("="*50)
            print(df.head(10))
            
            print("\n" + "="*50)
            print("WINNERS ONLY (Validation)")
            print("="*50)
            print(df[df['IS_WINNER'] == True][['MEMBER', 'GROUP', 'VOTES']])
    else:
        print("No Excel files found in the data directory to test.")
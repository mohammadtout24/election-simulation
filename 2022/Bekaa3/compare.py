import pandas as pd
import os

# 1. Define the file paths
data_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa3\Bekaa3_data.xlsx"
members_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa3\Bekaa3_members.xlsx"
output_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa3\Bekaa3_missing_report.xlsx"

def find_missing_members():
    # Check if files exist
    if not os.path.exists(data_path):
        print(f"Error: Data file not found at {data_path}")
        return
    if not os.path.exists(members_path):
        print(f"Error: Members file not found at {members_path}")
        return

    try:
        print("Loading files...")
        df_data = pd.read_excel(data_path)
        df_members = pd.read_excel(members_path)

        # ---------------- CONFIGURATION ----------------
        # Ensure these column names match your Excel files exactly
        data_col_name = 'MEMBER'
        member_col_name = 'MEMBER'
        # -----------------------------------------------

        # Create sets of names for faster comparison
        # We convert to string and strip whitespace to ensure clean matching
        data_names = set(df_data[data_col_name].dropna().astype(str).str.strip())
        member_names = set(df_members[member_col_name].dropna().astype(str).str.strip())

        # 1. Find members in YOUR LIST that are NOT in the DATA
        missing_from_data = list(member_names - data_names)

        # 2. Find members in DATA that are NOT in YOUR LIST
        missing_from_members = list(data_names - member_names)

        print(f"Analysis Complete:")
        print(f"- Members in your list but NOT in data file: {len(missing_from_data)}")
        print(f"- Members in data file but NOT in your list: {len(missing_from_members)}")

        # Create a DataFrame to save the results
        # Since lists might be different lengths, we create a dictionary first
        df_result = pd.DataFrame({
            'In_Members_File_But_Missing_From_Data': pd.Series(missing_from_data),
            'In_Data_File_But_Missing_From_Members': pd.Series(missing_from_members)
        })

        # Save to Excel
        df_result.to_excel(output_path, index=False)
        print(f"\nReport saved successfully to: {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    find_missing_members()
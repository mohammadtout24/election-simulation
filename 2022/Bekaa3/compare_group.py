import pandas as pd
import os

# 1. Define the file paths
data_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa2\Bekaa2_data.xlsx"
members_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa2\Bekaa2_members.xlsx"
output_path = r"C:\Users\QscUser\Desktop\LIMS\LIMS Election Project\code\DATA\Bekaa2\Bekaa2_group_list_compare.xlsx"

def compare_unique_groups():
    if not os.path.exists(data_path) or not os.path.exists(members_path):
        print("Error: One or more files not found.")
        return

    try:
        print("Loading files...")
        df_data = pd.read_excel(data_path)
        df_members = pd.read_excel(members_path)

        col_name = 'GROUP'

        # Get unique groups, convert to string, strip whitespace, and drop empty values
        groups_data = set(df_data[col_name].dropna().astype(str).str.strip())
        groups_members = set(df_members[col_name].dropna().astype(str).str.strip())

        # Calculate differences and intersection
        # Groups present in Data but NOT in Members file
        in_data_only = list(groups_data - groups_members)
        
        # Groups present in Members file but NOT in Data (potential typos in your list)
        in_members_only = list(groups_members - groups_data)
        
        # Groups present in BOTH
        common_groups = list(groups_data.intersection(groups_members))

        print(f"Analysis Complete:")
        print(f"- Unique Groups in Data Only: {len(in_data_only)}")
        print(f"- Unique Groups in Members Only: {len(in_members_only)}")
        print(f"- Common Groups: {len(common_groups)}")

        # Create a DataFrame to save results (handling different list lengths)
        df_result = pd.DataFrame({
            'Groups_Only_In_Data_File': pd.Series(in_data_only),
            'Groups_Only_In_Members_File': pd.Series(in_members_only),
            'Common_Groups': pd.Series(common_groups)
        })

        # Save to Excel
        df_result.to_excel(output_path, index=False)
        print(f"\nGroup comparison saved to: {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    compare_unique_groups()



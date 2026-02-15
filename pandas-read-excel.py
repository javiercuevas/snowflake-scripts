sheet_names = pd.read_excel(file_path).sheet_names
print(sheet_names)

reseller_df = pd.read_excel(file_path, sheet_name = "Reseller_Data")
display(reseller_df)

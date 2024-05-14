import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Download drug register
human_products = pd.read_json(
    'https://dati.zva.gov.lv/zalu-registrs/export/HumanProducts.json',
    encoding='utf-8-sig')

# Drop duplicate values
human_products.drop_duplicates(  # pylint: disable=E1101
    subset=['authorisation_no'],
    ignore_index=True,
    inplace=True)

# Drop columns that are not needed
human_products_columns = ['medicine_name',
                          'authorisation_no',
                          'pharmaceutical_form_lv',
                          'active_substance',
                          'short_name']
human_products.drop(  # pylint: disable=E1101
    columns=[col for col in human_products if col not in human_products_columns],
    inplace=True)


# Split drug register between EU registerd and other drugs
human_products_non_eu = human_products.drop(  # pylint: disable=E1101
    human_products[human_products['authorisation_no'].str.startswith('EU/')].index)

human_products_eu = human_products.drop(  # pylint: disable=E1101
    human_products[~human_products['authorisation_no'].str.startswith('EU/')].index)


# Drop duplicate values from EU registered medication
human_products_eu.drop_duplicates(  # pylint: disable=E1101
    subset=['medicine_name', 'pharmaceutical_form_lv'],
    ignore_index=True,
    inplace=True)

# Combine drug register back
human_products = pd.concat(
    [human_products_non_eu, human_products_eu], ignore_index=True)

# Fill empty cells with an empty string
human_products.fillna('', inplace=True)  # pylint: disable=E1101

# Download file with information on medication use in sports
doping_substances = pd.read_csv(  # break up long string with r''
    r'https://data.gov.lv/dati/dataset/'
    r'3635a536-9b05-4a29-8695-31269d30e7b0/resource/'
    r'ee8f9b14-1eee-494a-b7f5-6777a8232dcb/download')

# Drop products that are not currently registered
doping_substances = doping_substances[doping_substances['authorisation_no'].isin(
    human_products['authorisation_no'])]

# Add rows with medication that do not have information about use in sports
missing_medication = human_products[~human_products['authorisation_no']
                                    .isin(doping_substances['authorisation_no'])]
doping_substances = pd.concat(
    [doping_substances, missing_medication], ignore_index=True)

# Add column 'short_name' to doping_substances
doping_substances = pd.merge(left=doping_substances, right=human_products[[
                             'authorisation_no', 'short_name']], on='authorisation_no', how='left')

doping_substances.drop('short_name_x', axis=1, inplace=True)
doping_substances.rename(columns={'short_name_y': 'short_name'}, inplace=True)

# Fill empty cells with an empty string
doping_substances.fillna('', inplace=True)

# Try opening dataframe with information that was prepared the last time script was ran
# If file is found, then create dataframe with only newly added or recently edited medication
# If no file is found, then upload to Supabase the whole file
try:
    saved_in_supabase = pd.read_csv('saved_in_supabase.csv')
    saved_in_supabase.fillna('', inplace=True)
    df_to_upload = pd.concat([saved_in_supabase, doping_substances]).drop_duplicates(
        keep=False, ignore_index=True)
    df_to_upload.drop_duplicates(
        subset=['authorisation_no'],
        ignore_index=True,
        inplace=True,
        keep='last'
    )
except FileNotFoundError:
    df_to_upload = doping_substances

df_to_upload.to_csv('to_upload.csv', index=False)

# Initialize Supabase client
load_dotenv('.env')

url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY1') + \
    os.environ.get('SUPABASE_KEY2')+os.environ.get('SUPABASE_KEY3')
supabase: Client = create_client(url, key)

# Write data to Supabase
if not df_to_upload.empty:
    data, count = supabase.table('drug_reference').upsert(
        df_to_upload.to_dict(orient='records')).execute()
    print('Data written to Supabase!')
else:
    print('Nothing to write to Supabase')

# Save dataframe with latest information about medication use in sports to file
doping_substances.to_csv('saved_in_supabase.csv', index=False)

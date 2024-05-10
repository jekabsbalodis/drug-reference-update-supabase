import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd

# Download drug register
human_products = pd.read_json(
    'https://dati.zva.gov.lv/zalu-registrs/export/HumanProducts.json',
    encoding='utf-8-sig')

# Drop duplicate values
human_products.drop_duplicates(  # pylint: disable=E1101
    subset=['authorisation_no'],
    ignore_index=True,
    inplace=True)
human_products.drop_duplicates(  # pylint: disable=E1101
    subset=['medicine_name', 'pharmaceutical_form_lv'],
    ignore_index=True,
    inplace=True)

# Drop columns that are not needed
human_products_columns = ['medicine_name',
                          'authorisation_no',
                          'pharmaceutical_form_lv',
                          'active_substance']
human_products.drop(  # pylint: disable=E1101
    columns=[col for col in human_products if col not in human_products_columns],
    inplace=True)

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

# Fill empty cells with an empty string
doping_substances.fillna('', inplace=True)

# Try opening dataframe with information that was prepared the last time script was ran
# If file is found, then create dataframe with only newly added or recently edited medication
# If no file is found, then upload to Firestore the whole file
try:
    saved_in_firestore = pd.read_csv('saved_in_firestore.csv')
    saved_in_firestore.fillna('', inplace=True)
    df_to_upload = pd.concat([saved_in_firestore, doping_substances]).drop_duplicates(
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

# Initialize Firebase app
cred = credentials.Certificate(
    'drug-reference-firebase-adminsdk.json')
firebase_admin.initialize_app(cred)

# Get firestore db instance
db = firestore.client()

# Reference to Firestore collection
collection_ref = db.collection('drug_reference')

# Write data to Firestore
collection_ref = db.collection('drug_reference')
if not df_to_upload.empty:
    for row in df_to_upload.to_dict(orient='records'):
        doc_id = row['authorisation_no'].replace('/', '.')
        del row['authorisation_no']  # Remove ID from data dict
        collection_ref.document(doc_id).set(row)
    print('Data written to Firestore!')
else:
    print('Nothing to write to Firestore')

# Save dataframe with latest information about medication use in sports to file
doping_substances.to_csv('saved_in_firestore.csv', index=False)

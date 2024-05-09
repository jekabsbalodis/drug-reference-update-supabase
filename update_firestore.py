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
    r'ee8f9b14-1eee-494a-b7f5-6777a8232dcb/download'
)

# Fill empty cells with an empty string
doping_substances.fillna('', inplace=True)

# Drop products that are not currently registered
filtered_doping_substances = doping_substances[doping_substances['authorisation_no'].isin(
    human_products['authorisation_no'])]

# Add rows with medication that do not have information about use in sports
missing_medication = human_products[~human_products['authorisation_no']
                                    .isin(filtered_doping_substances['authorisation_no'])]
filtered_twice_doping_substances = pd.concat(
    [filtered_doping_substances, missing_medication], ignore_index=True)

# Fill empty cells with an empty string
filtered_twice_doping_substances.fillna('', inplace=True)

# print(human_products.shape)  # pylint: disable=E1101
# print(doping_substances.shape)
# print(filtered_doping_substances.shape)
# print(filtered_twice_doping_substances.shape)
# print(filtered_twice_doping_substances.shape)
# filtered_twice_doping_substances.to_csv('file.csv', index=False)

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
for row in filtered_twice_doping_substances.to_dict(orient='records'):
    doc_id = row['authorisation_no'].replace('/', '.')
    del row['authorisation_no']  # Remove ID from data dict
    collection_ref.document(doc_id).set(row)

print('Data written to Firestore!')

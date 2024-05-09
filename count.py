import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Initialize Firebase Admin SDK
cred = credentials.Certificate('drug-reference-aeed3-firebase-adminsdk-1c0l8-709a3f319a.json')
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Function to count documents in a collection
def count_documents(collection_name):
    collection_ref = db.collection(collection_name)
    docs = collection_ref.get()
    count = len(docs)
    return count

# Example usage
COLLECTION = 'drug_reference'
document_count = count_documents(COLLECTION)
print(f'The collection {COLLECTION} has {document_count} documents.')

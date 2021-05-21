import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("admin_credentials/firebase-creds.json")
firebase_admin.initialize_app(cred)
firestore_db = firestore.client()


def db():
    return firestore_db

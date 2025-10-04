from django.conf import settings
from pymongo import MongoClient

url = 'mongodb://localhost:27017'

client = MongoClient(settings.MONGO_URI)

db = client[settings.MONGO_DB_NAME]


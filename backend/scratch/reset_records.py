from pymongo import MongoClient
import os
from dotenv import load_dotenv

def reset():
    load_dotenv()
    client = MongoClient(os.environ['MONGO_URI'])
    db = client[os.environ['DB_NAME']]
    col = db[os.environ.get('COLLECTION_EXECUTION_INFO', 'Executioninfo')]
    
    # Reset records that are invalid but missing the field names
    res = col.update_many(
        {'isValid': False, 'invalidFields': []}, 
        {'$set': {'stage': 'validation failed'}}
    )
    print(f"Reset {res.modified_count} records for re-validation.")

if __name__ == '__main__':
    reset()

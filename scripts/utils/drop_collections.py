from pymongo import MongoClient
import psycopg2

# client = MongoClient("mongodb+srv://suman:suman@algotradingcluster.eszbc.mongodb.net/algotradingdb?retryWrites=true&w=majority")
# db = client.algotradingdb
#
# for col in db.collection_names():
#     db[col].drop()

conn = psycopg2.connect(host = 'localhost', database='algotrading', user='postgres', password='suman',port=5432)
cur = conn.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    cur.execute(f'DROP TABLE {table[0]}')
    print(f'{table[0]} dropped.')

conn.commit()
cur.close()
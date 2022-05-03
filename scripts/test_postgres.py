import psycopg2
import pandas.io.sql as psql

conn = None
try:
    conn = psycopg2.connect(host = 'localhost', database='algotrading', user='postgres', password='suman',port=5432)
    #cur = conn.cursor()
    print('PostgreSQL database version:')
    stock_df = psql.read_sql('SELECT * from stock',conn)
    print(stock_df)
    # db_version = cur.fetchall()
    # print(db_version)
    # cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
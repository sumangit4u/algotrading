import pandas as pd
import datetime
import psycopg2
import pandas.io.sql as psql
from scripts.nse_files import download_nse_files
from pymongo import MongoClient
from scripts.populate_nse_daily_data import populate_nse_files_data
from scripts.strategy import golden_cross,check_strategies

if __name__=='__main__':

    # client = MongoClient("mongodb+srv://suman:suman@algotradingcluster.eszbc.mongodb.net/algotradingdb?retryWrites=true&w=majority")
    # db = client.algotradingdb
    conn = psycopg2.connect(host = 'localhost', database='algotrading', user='postgres', password='suman',port=5432)
    #cur = conn.cursor()

    # for days in range(1940,0,-1):
    #     #download_nse_files(days)
    #     populate_nse_files_data(days)

    # filename = f"..\\symbols.txt"
    # with open(filename,'r') as fh:
    #     data = fh.read()
    # symbols = data.split('\n')
    #print(symbols)
    cur = conn.cursor()
    cur.execute(f'select distinct symbol from daily_fo_consolidated')
    for symbol in cur.fetchall():
        symbol=symbol[0]
        print(symbol)
        symbol_df = psql.read_sql(f"SELECT * from daily_equity where SYMBOL='{symbol}'", conn)
        symbol_df_w = psql.read_sql(f"SELECT * from weekly_equity where SYMBOL='{symbol}'", conn)
        symbol_df_m = psql.read_sql(f"SELECT * from monthly_equity where SYMBOL='{symbol}'", conn)
        daily_ind_df = psql.read_sql(f"SELECT * from daily_indicators where SYMBOL='{symbol}'", conn)
        check_strategies(symbol_df,symbol_df_w,symbol_df_m,daily_ind_df)
        break
    cur.close()
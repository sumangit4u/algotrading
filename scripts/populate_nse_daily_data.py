from pymongo import MongoClient
import pandas as pd
import numpy as np
import pprint
import psycopg2
import os
from datetime import datetime
import datetime as dtime
from scripts.cash_calc import *
from scripts.utils.db_utils import store_dataframe_to_postgre

def populate_nse_files_data(days=0):

    today_date = datetime.today().date() - dtime.timedelta(days=days)
    if today_date.isoweekday() < 6:
        print(today_date)
        year = today_date.year
        month = today_date.month
        date = today_date.day

        m = '%02d' % month
        d = '%02d' % date
        month_MMM = {'01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR', '05': 'MAY', '06': 'JUN', '07': 'JUL', '08': 'AUG',
                     '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'}

        # the connection uri to our course cluster
        # client = MongoClient("mongodb+srv://suman:suman@algotradingcluster.eszbc.mongodb.net/algotradingdb?retryWrites=true&w=majority")
        # db = client.algotradingdb
        conn = psycopg2.connect(host='localhost', database='algotrading', user='postgres', password='suman', port=5432)

        filename = f"..\\master_download_dump\\delivery_volumes\\{year}\\{year}{m}{d}.csv"
        if os.path.exists(filename):
            del_vol_df = pd.read_csv(filename, skiprows=[0, 1, 2])
            del_vol_df.reset_index(drop=True, inplace=True)
            del_vol_df = del_vol_df.loc[del_vol_df['Name of Security'] == 'EQ']
            del_vol_df = del_vol_df.iloc[:, [1, -1]]
            del_vol_df.rename(columns={'Sr No': 'SYMBOL', '% of Deliverable Quantity to Traded Quantity': '%Delivery'},
                              inplace=True)
            del_vol_df.columns = [col.lower() for col in del_vol_df.columns]

        filename = f"..\\master_download_dump\\nse_equity\\{year}\\cm{d}{month_MMM[m]}{year}bhav.csv"
        if os.path.exists(filename):
            equity_df = pd.read_csv(filename)
            equity_df.columns = [col.lower() for col in equity_df.columns]
            equity_df = equity_df.loc[equity_df['series'] == 'EQ']
            equity_df.reset_index(drop=True, inplace=True)

            cash_df = pd.merge(equity_df, del_vol_df, how='inner', on=['symbol'])
            cash_df['deliveryvalue_lakhs'] = np.round(cash_df['%delivery']*cash_df['tottrdval']/1e7,0)
            cash_df['tottrdqty'] = np.round(cash_df['tottrdqty'] / 100000, 3)
            cash_df['tottrdval'] = np.round(cash_df['tottrdval'] / 100000, 3)
            cash_df['timestamp'] = pd.to_datetime(cash_df['timestamp'])
            cash_df['week'] = cash_df['timestamp'].apply(lambda x: x.isocalendar()[1])
            cash_df['month'] = cash_df['timestamp'].dt.month
            cash_df['year'] = cash_df['timestamp'].dt.year
            cash_df.rename(columns={'%delivery':'deliverypercent','tottrdqty':'tottrdqty_lakhs','tottrdval':'tottrdval_lakhs'},inplace=True)
            cash_df = cash_df[['symbol','open','high','low','close','last','prevclose','tottrdqty_lakhs','tottrdval_lakhs',
                               'timestamp','totaltrades','deliverypercent','deliveryvalue_lakhs','week','month','year']]
            #db['daily_equity'].insert_many(cash_df.to_dict('records'))
            store_dataframe_to_postgre(cash_df,'daily_equity',conn)

            daily_indicator_df = calc_daily_indicators(cash_df)
            daily_indicator_df = daily_indicator_df[['symbol','timestamp','dailympf','dailyvwap']]
            #db['daily_indicators'].insert_many(daily_indicator_df.to_dict('records'))
            store_dataframe_to_postgre(daily_indicator_df, 'daily_indicators', conn)

            current_week = cash_df['week'][0]
            current_month = cash_df['month'][0]
            current_year = cash_df['year'][0]

            populate_weekly_data(current_week,current_year,conn)
            calc_weekly_indicators(current_week,current_year,conn)
            populate_monthly_data(current_month, current_year, conn)
            calc_monthly_indicators(current_month,current_year,conn)

        filename = f"..\\master_download_dump\\nse_f&o\\{year}\\fo{d}{month_MMM[m]}{year}bhav.csv"
        if os.path.exists(filename):
            fo_df = pd.read_csv(filename)
            fo_df.columns = [col.lower() for col in fo_df.columns]
            fo_df = fo_df.loc[fo_df['instrument'].isin(['FUTSTK', 'OPTSTK'])]
            fo_df['cashflow'] = np.round(
                (fo_df['open'] + fo_df['high'] + fo_df['low'] + fo_df['close']) / 4e8 * fo_df['chg_in_oi'], 2)
            fo_df['currval'] = np.round(fo_df['close'] * fo_df['open_int'] / 1e8,2)
            fo_df['series'] = pd.to_datetime(fo_df['expiry_dt']).dt.month - pd.to_datetime(fo_df['timestamp']).dt.month + 1
            fo_df_con = fo_df.groupby(['symbol', 'option_typ','timestamp'])['cashflow', 'currval', 'open_int'].agg('sum').reset_index()
            fo_df_ser = fo_df.groupby(['symbol', 'option_typ', 'series','timestamp'])['cashflow', 'currval', 'open_int'].agg('sum').reset_index()

            store_dataframe_to_postgre(fo_df_con,'daily_fo_consolidated',conn)
            store_dataframe_to_postgre(fo_df_ser, 'daily_fo_series', conn)
            # fo_con_dict = fo_df_con.to_dict('records')
            # db['daily_fo_consolidated'].insert_many(fo_con_dict)
            # fo_ser_dict = fo_df_ser.to_dict('records')
            # db['daily_fo_series'].insert_many(fo_ser_dict)

        filename= f"..\\master_download_dump\\indices\\{year}\\{year}{m}{d}.csv"
        if os.path.exists(filename):
            indices_df = pd.read_csv(filename)
            indices_df.replace('-',0,inplace=True)
            indices_df.columns = [''.join(col.split(' ')).lower() for col in indices_df.columns]
            indices_df['volume'] = indices_df['volume'].apply(lambda x:int(x)/100000)
            indices_df.rename(columns={'change(%)':'changepercent','turnover(rs.cr.)':'turnoverincrore',
                                       'volume':'volume_lakhs','p/e':'pe','p/b':'pb'},inplace=True)
            indices_df.reset_index(drop=True,inplace=True)
            #indices_dict = indices_df.to_dict('records')
            #db['daily_indices'].insert_many(indices_dict)
            store_dataframe_to_postgre(indices_df,'daily_indices',conn)

        filename = f"..\\master_download_dump\\participant_wise_open_interest\\{year}\\fao_participant_oi_{d}{m}{year}.csv"
        if os.path.exists(filename):
            participant_oi_df = pd.read_csv(filename,skiprows=[0])
            participant_oi_df.columns = [''.join(col.split(' ')).lower() for col in participant_oi_df.columns]
            participant_oi_df['Date'] = str(today_date)
            participant_oi_df.reset_index(drop=True,inplace=True)
            # participant_oi_dict = participant_oi_df.to_dict('records')
            # db['daily_participant_oi'].insert_many(participant_oi_dict)
            store_dataframe_to_postgre(participant_oi_df, 'daily_participant_oi', conn)

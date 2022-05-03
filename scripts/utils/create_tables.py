from pymongo import MongoClient
import psycopg2

# client = MongoClient("mongodb+srv://suman:suman@algotradingcluster.eszbc.mongodb.net/algotradingdb?retryWrites=true&w=majority")
# db = client.algotradingdb
#
# for col in db.collection_names():
#     db[col].drop()

conn = psycopg2.connect(host='localhost', database='algotrading', user='postgres', password='suman', port=5432)
cur = conn.cursor()
cur.execute("""create table daily_equity(SYMBOL VARCHAR(12),
                                         OPEN NUMERIC(9,2),
                                         HIGH NUMERIC(9,2),
                                         LOW NUMERIC(9,2),
                                         CLOSE NUMERIC(9,2),
                                         LAST NUMERIC(9,2),
                                         PREVCLOSE NUMERIC(9,2),
                                         TOTTRDQTY_LAKHS FLOAT,
                                         TOTTRDVAL_LAKHS NUMERIC(13,2),
                                         TIMESTAMP TIMESTAMP,
                                         TOTALTRADES INT,
                                         DELIVERYPERCENT NUMERIC(5,2),
                                         DELIVERYVALUE_LAKHS NUMERIC(13,2),
                                         WEEK NUMERIC(2),
                                         MONTH NUMERIC(2),
                                         YEAR NUMERIC(4))""")
cur.execute("""create table weekly_equity(SYMBOL VARCHAR(12),
                                         OPEN NUMERIC(9,2),
                                         LOW NUMERIC(9,2),                                         
                                         HIGH NUMERIC(9,2),
                                         CLOSE NUMERIC(9,2),
                                         WEEK NUMERIC(2),
                                         YEAR NUMERIC(4))""")
cur.execute("""create table monthly_equity(SYMBOL VARCHAR(12),
                                         OPEN NUMERIC(9,2),
                                         LOW NUMERIC(9,2),                                         
                                         HIGH NUMERIC(9,2),
                                         CLOSE NUMERIC(9,2),
                                         MONTH NUMERIC(2),
                                         YEAR NUMERIC(4))""")
cur.execute("""create table daily_indicators(SYMBOL VARCHAR(12),
                                         TIMESTAMP TIMESTAMP,
                                         DAILYMPF NUMERIC(9,2),                                         
                                         DAILYVWAP NUMERIC(9,2))""")
cur.execute("""create table weekly_indicators(SYMBOL VARCHAR(12),
                                         WEEK NUMERIC(2),
                                         YEAR NUMERIC(4),
                                         WEEKLYMPF NUMERIC(9,2),                                         
                                         WEEKLYVWAP NUMERIC(9,2))""")
cur.execute("""create table monthly_indicators(SYMBOL VARCHAR(12),
                                         MONTH NUMERIC(2),
                                         YEAR NUMERIC(4),
                                         MONTHLYMPF NUMERIC(9,2),                                         
                                         MONTHLYVWAP NUMERIC(9,2))""")
cur.execute("""create table daily_fo_consolidated(SYMBOL VARCHAR(12),
                                         OPTION_TYP VARCHAR(2),
                                         TIMESTAMP TIMESTAMP,
                                         CASHFLOW FLOAT,
                                         CURRVAL FLOAT,
                                         OPEN_INT INT)""")
cur.execute("""create table daily_fo_series(SYMBOL VARCHAR(12),
                                         OPTION_TYP VARCHAR(2),
                                         SERIES NUMERIC(2),
                                         TIMESTAMP TIMESTAMP,
                                         CASHFLOW FLOAT,
                                         CURRVAL FLOAT,
                                         OPEN_INT INT)""")
cur.execute("""create table daily_indices(INDEXNAME VARCHAR(50),
                                         INDEXDATE TIMESTAMP,
                                         OPENINDEXVALUE NUMERIC(9,2),
                                         HIGHINDEXVALUE NUMERIC(9,2),
                                         LOWINDEXVALUE NUMERIC(9,2),
                                         CLOSINGINDEXVALUE NUMERIC(9,2),
                                         POINTSCHANGE NUMERIC(6,2),
                                         CHANGEPERCENT NUMERIC(5,2),
                                         VOLUME_LAKHS INT,
                                         TURNOVERINCRORE NUMERIC(8,2),
                                         PE NUMERIC(6,2),
                                         PB NUMERIC(4,2),
                                         DIVYIELD NUMERIC(5,2))""")
cur.execute("""create table daily_participant_oi(CLIENTTYPE VARCHAR(50),
                                         FUTUREINDEXLONG INT,
                                         FUTUREINDEXSHORT INT,
                                         FUTURESTOCKLONG INT,
                                         FUTURESTOCKSHORT INT,
                                         OPTIONINDEXCALLLONG INT,
                                         OPTIONINDEXPUTLONG INT,
                                         OPTIONINDEXCALLSHORT INT,
                                         OPTIONINDEXPUTSHORT INT,
                                         OPTIONSTOCKCALLLONG INT,
                                         OPTIONSTOCKPUTLONG INT,
                                         OPTIONSTOCKCALLSHORT INT,
                                         OPTIONSTOCKPUTSHORT INT,
                                         TOTALLONGCONTRACTS INT,
                                         TOTALSHORTCONTRACTS INT,
                                         DATE TIMESTAMP)""")
conn.commit()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    print(table)

cur.close()
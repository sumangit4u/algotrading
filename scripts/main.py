import pandas as pd
import datetime
from scripts.utils.wrappers import TestApp
from scripts.utils.divergence import calc_rsi,check_divergence

if __name__=='__main__':
    app = TestApp('d',file='../symbols.txt')
    app.connect("127.0.0.1", 7496, clientId=123)
    app.run()

    df = pd.DataFrame(app.data, columns=['DateTime', 'Close','Volume','Symbol'])
    df['DateTime'] = pd.to_datetime(df['DateTime'])
    rsi_df = calc_rsi(df,app.symbols)

    for symbol in app.symbols:
        symbol_rsi_df = rsi_df.loc[rsi_df['Symbol']==symbol].reset_index(drop = True)
        out_df = check_divergence(symbol_rsi_df,order = 3)
        out_df = check_divergence(symbol_rsi_df,order = 5)

    out_df['Gap'] = out_df['Date2'] - out_df['Date1']
    print(out_df.loc[out_df['Date2'] >= pd.to_datetime(datetime.date.today()) + datetime.timedelta(days=-1)].sort_values(
        by='Gap'))

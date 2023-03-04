from scripts.utils.indicators_as_you_go import *
import pandas as pd
pd.options.display.width = 0
pd.set_option('display.max_columns', 500)

# def golden_cross(symbol_df,daily_ind_df):
#     symbol_df = calc_sma(symbol_df, 5)
#     symbol_df = calc_ema(symbol_df, 5)
#     symbol_df = calc_rsi(symbol_df)
#     symbol_df = calc_bband(symbol_df,period=5)
#     symbol_df = calc_ndays_vwap(symbol_df,daily_ind_df,5)
#     symbol_df = calc_sma(symbol_df, 9,'high')
#     print(symbol_df.head(20))

def check_strategies(symbol_df,symbol_df_w,symbol_df_m,daily_ind_df):
    symbol_df = calc_ndays_vwap(symbol_df, daily_ind_df, 5)
    symbol_df = calc_sma(symbol_df, 20)
    symbol_df = calc_sma(symbol_df, 50)
    symbol_df = calc_sma(symbol_df, 124)
    symbol_df = calc_sma(symbol_df, 150)
    symbol_df = calc_sma(symbol_df, 200)
    symbol_df = calc_sma(symbol_df, 9,'high')
    symbol_df = calc_ema(symbol_df, 5)
    symbol_df = calc_ema(symbol_df, 15)
    symbol_df = calc_ema(symbol_df, 20)
    symbol_df_w = calc_ema(symbol_df_w, 5)
    symbol_df_w = calc_ema(symbol_df_w, 15)
    symbol_df_w = calc_ema(symbol_df_w, 20)
    symbol_df_m = calc_ema(symbol_df_m, 5)
    symbol_df_m = calc_ema(symbol_df_m, 15)
    symbol_df_m = calc_ema(symbol_df_m, 20)
    symbol_df = calc_ema(symbol_df, 50,'high')
    symbol_df = calc_ema(symbol_df, 50,'low')
    symbol_df_w = calc_ema(symbol_df_w, 50,'high')
    symbol_df_w = calc_ema(symbol_df_w, 50,'low')
    symbol_df_m = calc_ema(symbol_df_m, 50,'high')
    symbol_df_m = calc_ema(symbol_df_m, 50,'low')
    symbol_df = calc_ema(symbol_df, 20)
    symbol_df_BBand_m = calc_bband(symbol_df_m,'m')
    symbol_df_BBand_w = calc_bband(symbol_df_w,'w')


    # Bearish CrossOver - One day before 5 EMA was higher than 20 EMA but today it has closed below 20 EMA
    EMA_5_lower_EMA_20 = symbol_df['ema_5_close'] < symbol_df['ema_20_close']
    symbol_df['Bearish_crossover'] = (EMA_5_lower_EMA_20 == True) & (EMA_5_lower_EMA_20.shift(1) == False)

    # Bullish CrossOver - One day before 5 EMA was lower than 20 EMA but today it has closed above 20 EMA
    EMA_5_lower_EMA_20 = symbol_df['ema_5_close'] < symbol_df['ema_20_close']
    symbol_df['Bullish_crossover'] = (EMA_5_lower_EMA_20 == False) & (EMA_5_lower_EMA_20.shift(1) == True)


    symbol_df['Triveni_Sangam'] = (abs(symbol_df['sma_20_close'] - symbol_df['5days_vwap'])<=0.005*symbol_df['close']) & \
                                  (abs(symbol_df['sma_9_high']-symbol_df['5days_vwap'])<=0.005*symbol_df['close'])

    symbol_df['DMA_Convergence'] = (abs(symbol_df['sma_20_close'] - symbol_df['sma_50_close']) <= 0.001 * symbol_df['close']) & \
                                   (abs(symbol_df['sma_20_close'] - symbol_df['sma_124_close']) <= 0.001 * symbol_df['close']) & \
                                   (abs(symbol_df['sma_20_close'] - symbol_df['sma_150_close']) <= 0.001 * symbol_df['close']) & \
                                   (abs(symbol_df['sma_20_close'] - symbol_df['sma_200_close']) <= 0.001 * symbol_df['close'])


    # Checking if the daily close is breaching the final value of monthly BB. Note final vale of monthly BB is
    # calculated at the close of month.
    symbol_df['Close_Below_MonthlyBB'] = ''
    symbol_df['Close_Above_MonthlyBB'] = ''
    merged_with_month = symbol_df.merge(symbol_df_BBand_m[['month','year','bband_upper_m','bband_lower_m']], how='left',on=['month','year'])
    symbol_df['Close_Below_MonthlyBB'] = merged_with_month['close'] < merged_with_month['bband_lower_m']
    symbol_df['Close_Above_MonthlyBB'] = merged_with_month['close'] > merged_with_month['bband_upper_m']

    # Checking if the daily close is breaching the final value of weekly BB. Note final vale of weekly BB is
    # calculated at the close of week.
    symbol_df['Close_Below_WeeklyBB'] = ''
    symbol_df['Close_Above_WeeklyBB'] = ''
    merged_with_week = symbol_df.merge(symbol_df_BBand_w[['week','year','bband_upper_w','bband_lower_w']], how='left',on=['week','year'])
    symbol_df['Close_Below_WeeklyBB'] = merged_with_week['close'] < merged_with_week['bband_lower_w']
    symbol_df['Close_Above_WeeklyBB'] = merged_with_week['close'] > merged_with_week['bband_upper_w']

    merged_with_week = symbol_df.merge(symbol_df_w[['week', 'year', 'ema_50_high','ema_50_low','ema_15_close',
                                                    'ema_5_close','ema_20_close']], how='left', on=['week', 'year'])
    merged_with_month = symbol_df.merge(symbol_df_m[['month', 'year', 'ema_50_high','ema_50_low','ema_15_close',
                                                    'ema_5_close','ema_20_close']], how='left', on=['month', 'year'])

    symbol_df['ATFEMA_15C_Above_50H_and_50L'] = (merged_with_week['ema_15_close_y'] > merged_with_week['ema_50_high_y']) & \
                                         (merged_with_week['ema_15_close_y'] > merged_with_week['ema_50_low_y']) & \
                                         (merged_with_month['ema_15_close_y'] > merged_with_month['ema_50_high_y']) & \
                                         (merged_with_month['ema_15_close_y'] > merged_with_month['ema_50_low_y']) & \
                                         (symbol_df['ema_15_close'] > symbol_df['ema_50_high']) & \
                                         (symbol_df['ema_15_close'] > symbol_df['ema_50_low'])

    symbol_df['ATFEMA_15C_Below_50H_and_50L'] = (merged_with_week['ema_15_close_y'] < merged_with_week['ema_50_high_y']) & \
                                         (merged_with_week['ema_15_close_y'] < merged_with_week['ema_50_low_y']) & \
                                         (merged_with_month['ema_15_close_y'] < merged_with_month['ema_50_high_y']) & \
                                         (merged_with_month['ema_15_close_y'] < merged_with_month['ema_50_low_y']) & \
                                         (symbol_df['ema_15_close'] < symbol_df['ema_50_high']) & \
                                         (symbol_df['ema_15_close'] < symbol_df['ema_50_low'])

    symbol_df['MonthlyEMA_15C_Below_50H_and_50L'] = (merged_with_month['ema_15_close_y'] < merged_with_month['ema_50_high_y']) & \
                                         (merged_with_month['ema_15_close_y'] < merged_with_month['ema_50_low_y'])

    symbol_df['WeeklyEMA_15C_Below_50H_and_50L'] = (merged_with_week['ema_15_close_y'] < merged_with_week['ema_50_high_y']) & \
                                         (merged_with_week['ema_15_close_y'] < merged_with_week['ema_50_low_y'])

    symbol_df['DailyEMA_15C_Below_50H_and_50L'] = (symbol_df['ema_15_close'] < symbol_df['ema_50_high']) & \
                                            (symbol_df['ema_15_close'] < symbol_df['ema_50_low'])

    symbol_df['MonthlyEMA_15C_Above_50H_and_50L'] = (merged_with_month['ema_15_close_y'] > merged_with_month['ema_50_high_y']) & \
                                         (merged_with_month['ema_15_close_y'] > merged_with_month['ema_50_low_y'])

    symbol_df['WeeklyEMA_15C_Above_50H_and_50L'] = (merged_with_week['ema_15_close_y'] > merged_with_week['ema_50_high_y']) & \
                                         (merged_with_week['ema_15_close_y'] > merged_with_week['ema_50_low_y'])

    symbol_df['DailyEMA_15C_Above_50H_and_50L'] = (symbol_df['ema_15_close'] > symbol_df['ema_50_high']) & \
                                            (symbol_df['ema_15_close'] > symbol_df['ema_50_low'])

    symbol_df['MonthlyEMA_5C_Above_20C'] = (merged_with_month['ema_5_close_y'] > merged_with_month['ema_20_close_y'])
    symbol_df['WeeklyEMA_5C_Above_20C'] = (merged_with_week['ema_5_close_y'] > merged_with_week['ema_20_close_y'])
    symbol_df['DailyEMA_5C_Above_20C'] = (symbol_df['ema_5_close'] > symbol_df['ema_20_close'])

    print(symbol_df.tail(200))
    #symbol_df.to_csv(f"..\\strategies_by_symbol\\{symbol_df['symbol'][0]}.csv")
    #symbol_df.to_json(f"..\\strategies_by_symbol\\{symbol_df['symbol'][0]}.json")
    symbol_df.to_excel(f"..\\strategies_by_symbol\\{symbol_df['symbol'][0]}.xlsx")
    # Testing branches







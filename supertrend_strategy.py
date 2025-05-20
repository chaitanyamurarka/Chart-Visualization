# supertrend_strategy.py
import pandas as pd
import numpy as np

def calculate_atr(df, period=14):
    """
    Calculates Average True Range (ATR).
    Args:
        df (pd.DataFrame): DataFrame with 'high', 'low', 'close' columns.
        period (int): The ATR period.
    Returns:
        pd.Series: ATR values.
    """
    if df.empty or not all(col in df.columns for col in ['high', 'low', 'close']):
        return pd.Series(dtype=float, index=df.index) # Return empty series with same index

    df_copy = df.copy()
    df_copy['h-l'] = df_copy['high'] - df_copy['low']
    df_copy['h-pc'] = abs(df_copy['high'] - df_copy['close'].shift(1))
    df_copy['l-pc'] = abs(df_copy['low'] - df_copy['close'].shift(1))
    
    # The first value of h-pc and l-pc will be NaN due to shift(1)
    # For TR calculation, we can fill these NaNs with h-l for the first row.
    # However, typical ATR calculation starts after the first 'period' bars anyway.
    df_copy['tr'] = df_copy[['h-l', 'h-pc', 'l-pc']].max(axis=1)
    
    # Using Exponential Moving Average for ATR (Wilder's smoothing)
    # alpha = 1/period makes it equivalent to Wilder's RMA if com=period-1
    atr = df_copy['tr'].ewm(com=period-1, min_periods=period, adjust=False).mean()
    return atr

def calculate_supertrend(df, period=10, multiplier=3):
    """
    Calculates the Supertrend indicator.
    Args:
        df (pd.DataFrame): DataFrame with 'time', 'high', 'low', 'close' columns.
                           'time' should be epoch seconds.
        period (int): The ATR period for Supertrend.
        multiplier (float): The ATR multiplier.
    Returns:
        pd.DataFrame: DataFrame with 'time', 'supertrend', 'supertrend_direction', 
                      'buy_signal_time', 'sell_signal_time' columns.
    """
    if df.empty or not all(col in df.columns for col in ['time', 'high', 'low', 'close']):
        return pd.DataFrame(columns=['time', 'supertrend', 'supertrend_direction', 'buy_signal_time', 'sell_signal_time'])

    df_st = df.copy()
    df_st['atr'] = calculate_atr(df_st, period=period)

    # Ensure ATR is valid before proceeding
    if df_st['atr'].isnull().all() or df_st['atr'].dropna().empty:
        df_st['supertrend'] = np.nan
        df_st['supertrend_direction'] = 'unknown'
        df_st['buy_signal_time'] = np.nan
        df_st['sell_signal_time'] = np.nan
        return df_st[['time', 'supertrend', 'supertrend_direction', 'buy_signal_time', 'sell_signal_time']]

    # Basic Supertrend calculation
    hl2 = (df_st['high'] + df_st['low']) / 2
    df_st['upperband'] = hl2 + (multiplier * df_st['atr'])
    df_st['lowerband'] = hl2 - (multiplier * df_st['atr'])
    
    df_st['in_uptrend'] = True # Initialize
    df_st['supertrend'] = 0.0  # Initialize

    # Determine initial trend based on first valid data point with ATR
    first_valid_idx = df_st['atr'].first_valid_index()
    if first_valid_idx is None: # Should be caught by previous check, but as a safeguard
        df_st['supertrend'] = np.nan
        df_st['supertrend_direction'] = 'unknown'
        df_st['buy_signal_time'] = np.nan
        df_st['sell_signal_time'] = np.nan
        return df_st[['time', 'supertrend', 'supertrend_direction', 'buy_signal_time', 'sell_signal_time']]

    # Initialize supertrend for the first valid ATR row
    # Ensure we use .loc for setting values to avoid SettingWithCopyWarning
    if df_st.loc[first_valid_idx, 'close'] > df_st.loc[first_valid_idx, 'lowerband']:
        df_st.loc[first_valid_idx, 'in_uptrend'] = True
        df_st.loc[first_valid_idx, 'supertrend'] = df_st.loc[first_valid_idx, 'lowerband']
    else:
        df_st.loc[first_valid_idx, 'in_uptrend'] = False
        df_st.loc[first_valid_idx, 'supertrend'] = df_st.loc[first_valid_idx, 'upperband']


    for current in range(first_valid_idx + 1, len(df_st)):
        previous = current - 1
        
        # Ensure ATR is not NaN for calculation, if it is, carry forward previous supertrend and trend
        if pd.isna(df_st.loc[current, 'atr']):
            df_st.loc[current, 'supertrend'] = df_st.loc[previous, 'supertrend']
            df_st.loc[current, 'in_uptrend'] = df_st.loc[previous, 'in_uptrend']
            continue

        if df_st.loc[previous, 'in_uptrend']: # If previous trend was up
            if df_st.loc[current, 'close'] < df_st.loc[previous, 'supertrend']:
                df_st.loc[current, 'in_uptrend'] = False
            else:
                df_st.loc[current, 'in_uptrend'] = True
        else: # If previous trend was down
            if df_st.loc[current, 'close'] > df_st.loc[previous, 'supertrend']:
                df_st.loc[current, 'in_uptrend'] = True
            else:
                df_st.loc[current, 'in_uptrend'] = False

        if df_st.loc[current, 'in_uptrend']:
            df_st.loc[current, 'supertrend'] = df_st.loc[current, 'lowerband']
            if df_st.loc[previous, 'in_uptrend']: # If still in uptrend, ST can only go up or stay same
                 df_st.loc[current, 'supertrend'] = max(df_st.loc[current, 'supertrend'], df_st.loc[previous, 'supertrend'])
        else: # In downtrend
            df_st.loc[current, 'supertrend'] = df_st.loc[current, 'upperband']
            if not df_st.loc[previous, 'in_uptrend']: # If still in downtrend, ST can only go down or stay same
                 df_st.loc[current, 'supertrend'] = min(df_st.loc[current, 'supertrend'], df_st.loc[previous, 'supertrend'])

    df_st['buy_signal_time'] = np.nan
    df_st['sell_signal_time'] = np.nan

    for i in range(first_valid_idx + 1, len(df_st)):
        if df_st.loc[i, 'in_uptrend'] and not df_st.loc[i-1, 'in_uptrend']:
            df_st.loc[i, 'buy_signal_time'] = df_st.loc[i, 'time']
        elif not df_st.loc[i, 'in_uptrend'] and df_st.loc[i-1, 'in_uptrend']:
            df_st.loc[i, 'sell_signal_time'] = df_st.loc[i, 'time']
            
    df_st['supertrend_direction'] = df_st['in_uptrend'].apply(lambda x: 'up' if x else 'down')
    df_st['supertrend'] = df_st['supertrend'].astype(float)

    return df_st[['time', 'supertrend', 'supertrend_direction', 'buy_signal_time', 'sell_signal_time']]

# Example Usage (for testing this file independently)
if __name__ == '__main__':
    data = {
        'time': pd.to_datetime(['2023-01-01 09:15:00', '2023-01-01 09:16:00', '2023-01-01 09:17:00',
                                '2023-01-01 09:18:00', '2023-01-01 09:19:00', '2023-01-01 09:20:00',
                                '2023-01-01 09:21:00', '2023-01-01 09:22:00', '2023-01-01 09:23:00',
                                '2023-01-01 09:24:00', '2023-01-01 09:25:00', '2023-01-01 09:26:00',
                                '2023-01-01 09:27:00', '2023-01-01 09:28:00', '2023-01-01 09:29:00',
                                '2023-01-01 09:30:00']).astype(np.int64) // 10**9,
        'open': [100, 101, 102, 103, 104, 103, 102, 100, 98, 99, 101, 100, 102, 103, 105, 104],
        'high': [102, 103, 104, 105, 106, 105, 104, 102, 100, 101, 102, 101, 103, 105, 106, 105],
        'low':  [99, 100, 101, 102, 103, 102, 101, 99, 97, 98, 100, 99, 101, 102, 104, 103],
        'close':[101, 102, 103, 104, 103, 102, 100, 98, 99, 101, 100, 102, 103, 105, 104, 102],
        'volume':[1000, 1200, 1100, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 1500, 1600, 1700, 1800, 1900]
    }
    df = pd.DataFrame(data)
    
    supertrend_df = calculate_supertrend(df.copy(), period=7, multiplier=2)
    print("Supertrend Data:")
    print(supertrend_df)
    print("\nBuy Signals (time):")
    print(supertrend_df[supertrend_df['buy_signal_time'].notna()]['buy_signal_time'])
    print("\nSell Signals (time):")
    print(supertrend_df[supertrend_df['sell_signal_time'].notna()]['sell_signal_time'])
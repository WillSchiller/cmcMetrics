import boto3
from datetime import datetime
import pandas as pd
import io
from datetime import timedelta, datetime
from talipp.indicators import EMA, SMA, Stoch
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator # TODO add volume data
from ta.trend import MACD

s3_client = boto3.client('s3')
current_date = datetime.now()
history = pd.DataFrame()
current_slice = pd.DataFrame()



#get data from s3
for i in range(0, 14):
    date = (current_date - timedelta(hours=i)).strftime('%Y/%m/%d/%H')
    file = f'cmc/latest/{date}/cmcdata.csv'
    csv_data = s3_client.get_object(Bucket='gascity', Key=file)
    csv_content = csv_data['Body'].read()
    data  = pd.read_csv(io.BytesIO(csv_content))
    if i == 0:
        current_slice = data
    history = pd.concat([history, data]).reset_index(drop=True)

current_slice['EMA'] = pd.Series(0.0, index=current_slice.index, dtype='float64')
current_slice['RSI'] = pd.Series(0.0, index=current_slice.index, dtype='float64')
current_slice['MACD'] = pd.Series(0.0, index=current_slice.index, dtype='float64')



for index, group in history.groupby(['symbol', 'name', 'date_added']):
    group = group.sort_values(by='timestamp')
    if len(group['price']) < 14:
        group['price'] = group['price'].interpolate(method='linear', limit_direction='forward', axis=0)
        values = group['price'].values
    else:
        values = group['price'].values
    try:
        rsi_indicator = RSIIndicator(close=group['price'], window=14, fillna=False)
        macd = MACD(close=group['price'], window_slow=14, window_fast=6, window_sign=3, fillna= False)
        #obv_indicator = OnBalanceVolumeIndicator(close=group['price'], volume=your_data['Volume'], fillna=False)
        current_slice.loc[(current_slice['symbol'] == index[0]) & (current_slice['name'] == index[1]) & (current_slice['date_added'] == index[2]), 'EMA'] = EMA(period=14, input_values=values)
        current_slice.loc[(current_slice['symbol'] == index[0]) & (current_slice['name'] == index[1]) & (current_slice['date_added'] == index[2]), 'RSI'] = rsi_indicator.rsi()
        current_slice.loc[(current_slice['symbol'] == index[0]) & (current_slice['name'] == index[1]) & (current_slice['date_added'] == index[2]), 'MACD'] = macd.macd()
    except Exception as e:
        print(e)
        continue
current_slice.to_csv('ema.csv', index=False)
print(current_slice)




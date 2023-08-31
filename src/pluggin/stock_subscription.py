from vnstock import *
import os
from google.cloud import pubsub_v1
import datetime
from json import loads, dumps
from fastavro import writer, parse_schema, reader
from google.cloud import bigquery
import json
import pytz
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/nguyentuanduong7/airflow/key')))
from keys import bot_token,chat_id


current_time= datetime.datetime.now(pytz.timezone('Asia/Ho_Chi_Minh') )
Datetime=current_time.strftime("%Y-%m-%d %H:%M:%S")
credentials_path = '/home/nguyentuanduong7/airflow/key/credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path
bot_token=bot_token()
chat_id=chat_id()
publisher = pubsub_v1.PublisherClient()
topic_path= 'projects/vnstock-pipeline/topics/vnstockpubsub'
sum_stock = price_depth(stock_list='ACB,TCB,FPT,FOX')

sum_stocks=pd.concat([sum_stock,pd.DataFrame(columns = ['Datetime','Ticker','Reference','Ceiling','Floor','Matched','Volume'])])
sum_stocks['Datetime'] = Datetime

sum_stocks_df=sum_stocks[['Mã CP','Giá tham chiếu','Giá Trần','Giá Sàn','Giá khớp lệnh','Tổng Khối Lượng']]
sum_stocks_df['Datetime']=sum_stocks['Datetime'].astype(str)

sum_stocks_df.rename(columns={'Mã CP':'Ticker',
                        'Giá tham chiếu':'Reference',
                        'Giá Trần':'Ceiling',
                        'Giá Sàn':'Floor',
                        'Giá khớp lệnh':'Matched',
                        'Tổng Khối Lượng':'Volume'
                        },inplace=True)
print(sum_stocks_df)

def vm_pub_pubsub_1H():

    for row_index,row in sum_stocks_df.iterrows():
        data_dict = row.to_dict()
        message = json.dumps(data_dict,ensure_ascii=False)
        message_str = message.encode('utf-8')        
        future = publisher.publish(topic_path,message_str)
        print(f'publish message id {future.result()}')


def vm_pub_pubsub_1M():
    matched_values=len(sum_stocks_df)
    expectation_price = 50000
    Selected_stock="ACB,TCB,FPT,FOX"
    count=0
    for i in range(len(sum_stocks_df)):
        matched_value = sum_stocks_df.iloc[i]['Matched']
        matched_name = sum_stocks_df.iloc[i]['Ticker']
        Reference_price = sum_stocks_df.iloc[i]['Reference']
        Limit=(matched_value*0.1) + matched_value

        if matched_value < Limit and matched_name in Selected_stock:
            count+=1
            selected_stock = sum_stocks_df.iloc[i][['Ticker','Matched']].to_string()
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={count}/{len(sum_stocks_df)}. Selected_stock < 10% expectation_price({Limit})  at {current_time.strftime('%Y-%m-%d %H:%M:%S')}: \n{selected_stock}"
            response = requests.get(url)
            print(selected_stock)

vm_pub_pubsub_1M()
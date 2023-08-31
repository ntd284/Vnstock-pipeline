from vnstock import *
#Get list of available stock code on market.
import csv
import logging
import glob
import subprocess
import os
from datetime import datetime
start_date=last_xd(365)
current_date=today()
print(start_date)
print(current_date)

def stock_collection_1Y():
    
    File=f'/home/nguyentuanduong7/airflow/data/stock_data_1Y.csv'
    f = open(File, "w")
  
    lists_company = listing_companies()
    stocks=lists_company['ticker']
    list_index=[ 'VNINDEX', 'VN30', 'HNX', 'HNX30', 'UPCOM', 'VNXALLSHARE', 'VN30F1M', 'VN30F2M', 'VN30F1Q', 'VN30F2Q']
    count=0

    resolution="1D"
    with open(File, 'w') as writer:
        Headers=['time', 'open', 'high','low', 'close','volume','ticker','type']
        writer.write(','.join(Headers)+'\n')
        for stock in stocks:
            try:
                stock_data=stock_historical_data (symbol=stock, start_date=start_date, end_date=current_date, resolution=resolution, type='stock')
                stock_data=pd.concat([stock_data,pd.DataFrame(columns = ['type'])])
                stock_data['type']= "stock"
                count+=1
                print(f"{len(stocks)-count}/{len(stocks)}_{stock}")
                stock_data.to_csv(writer,index=False,header=False)
                if count == 50:
                    return "Done"
            except:
                count+=1
                print(f"{len(stocks)-count}_error_{len(stocks)}_{stock}")
                logging.exception("message")

        for index in list_index:
            try:
                index_data=stock_historical_data (symbol=index, start_date=start_date, end_date=current_date, resolution=resolution, type='index')
                stock_data=pd.concat([stock_data,pd.DataFrame(columns = ['type'])])
                stock_data['type']= "index"
                count+=1
                print(f"{len(stocks)-count}/{len(stocks)}_{stock}")
                stock_dates.to_csv(writer,index=False,header=False)
                print(f"{len(stocks)-count}/{len(stocks)}_{stock}")
                if count == 10:
                    return "Done"
            except:
                count+=1
                print(f"{len(stocks)-count}_error_{len(stocks)}_{stock}")
                logging.exception("message")
#'2023-06-01'
def stock_collection_1D():
    list_index=[ 'VNINDEX', 'VN30', 'HNX', 'HNXstoc30', 'UPCOM', 'VNXALLSHARE', 'VN30F1M', 'VN30F2M', 'VN30F1Q', 'VN30F2Q']
    lists_company = listing_companies()
    stocks=lists_company['ticker']
    resolution = '1H'
    count=0
    with open(f'/home/nguyentuanduong7/airflow/data/stock_data_1Y.csv', 'w',encoding="utf-8") as writer:
        Headers=['time', 'open', 'high','low', 'close','volume','ticker','type']
        writer.write(','.join(Headers)+'\n')
        for stock in stocks:
            try:
                stock_data=stock_historical_data (symbol=stock, start_date=current_date, end_date=current_date, resolution=resolution, type='stock')
                stock_data=pd.concat([stock_data,pd.DataFrame(columns = ['type','Datetime'])])
                stock_data['type']= "stock"
                stock_data['time'] = pd.to_datetime(stock_data['time'], format='%Y-%m-%d %H:%M:%S%z')
                stock_data['Datetime']=stock_data['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                stock_dates=stock_data.drop(columns={'time'})
                print(stock_dates)
                count+=1
                stock_dates.to_csv(writer,index=False,header=False)
                print(f"{len(stocks)-count}/{len(stocks)}_{stock}")
                if count == 10:
                    return "Done"
            except:
                count+=1
                print(f"{len(stocks)-count}_error_{len(stocks)}_{stock}")
                logging.exception("message")
                
          
        for index in list_index:
            try:
                stock_data=stock_historical_data (symbol=stock, start_date=current_date, end_date=current_date, resolution=resolution, type='stock')
                stock_data=pd.concat([stock_data,pd.DataFrame(columns = ['type','Datetime'])])
                stock_data['type']= "index"
                stock_data['time'] = pd.to_datetime(stock_data['time'], format='%Y-%m-%d %H:%M:%S%z')
                stock_data['Datetime']=stock_data['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                stock_dates=stock_data.drop(columns={'time'})
                print(stock_dates)
                count+=1
                stock_dates.to_csv(writer,index=False,header=False)
                print(f"{len(stocks)-count}/{len(stocks)}_{stock}")
                if count == 10:
                    return "Done"
            except:
                count+=1
                print(f"{len(stocks)-count}_error_{len(stocks)}_{stock}")
                logging.exception("message")
        
def send_gsc_1D():

    folder_path = "/home/nguyentuanduong7/airflow/data"
    extension = f"stock_data_1D.csv"
    file_list = glob.glob(os.path.join(folder_path, extension))
    for filename in file_list:
        bucket_name = "vnstock-storage"
        gcs_name = filename.replace("/home/nguyentuanduong7/airflow/data/", "")
        gsutil_command = [
            "gsutil",
            "-o",
            "GSUtil:parallel_composite_upload_threshold=150M",
            "-m",
            "cp",
            filename,
            f"gs://{bucket_name}/stock-index-storage-1D/{gcs_name}",
        ]
        subprocess.run(gsutil_command, check=True)
        print("done_send_gsc_1D")

def send_gsc_1Y():
    folder_path = "/home/nguyentuanduong7/airflow/data"
    extension = f"stock_data_1Y.csv"
    file_list = glob.glob(os.path.join(folder_path, extension))
    for filename in file_list:
        bucket_name = "vnstock-storage"
        gcs_name = filename.replace("/home/nguyentuanduong7/airflow/data/", "")
        gsutil_command = [
            "gsutil",
            "-o",
            "GSUtil:parallel_composite_upload_threshold=150M",
            "-m",
            "cp", 
            filename,
            f"gs://{bucket_name}/stock-index-storage_1Y/{gcs_name}",
        ]
        subprocess.run(gsutil_command, check=True)
        print("done_send_gsc_1Y")

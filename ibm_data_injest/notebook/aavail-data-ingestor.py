

import os
import sys
import argparse
import pandas as pd
import numpy as np
import sqlite3
import datetime

DATA_DIR = os.path.join("..","data")


def connect_db(file_path):
    """
    function to connection to aavail database
    INPUT : the file path of the data base
    OUTPUT : the sqlite connection to the database
    """
    ## YOUR CODE HERE
    try:
        conn = sqlite3.connect(os.path.join(DATA_DIR,file_path))
        print("...successfully connected to db\n")
    except :
        print("...unsuccessful connection\n")
    
    return conn

def ingest_db_data(conn):
    """
    load and clean the db data
    INPUT : the sqlit connection to the databse
    OUPUT : the customer dataframe
    """
    ## YOUR CODE HERE
    customer_query = '''
        SELECT cus.customer_id,cus.last_name,cus.first_name,cus.DOB,
        cus.city,cus.state,country.country_name 
        FROM CUSTOMER cus
        LEFT JOIN COUNTRY on cus.country_id=country.country_id;
        
    '''
    customers = pd.read_sql_query(customer_query,conn)
    df_db = customers.drop_duplicates(subset=['customer_id'])
    
    return df_db

def ingest_stream_data(file_path):
    """
    load and clean the stream data
    INPUT : the file path of the stream csv
    OUTPUT : the streams dataframe and a mapping of the customers that churned
    """
    ## YOUR CODE HERE
    df_streams = pd.read_csv(os.path.join(DATA_DIR,file_path))
    df_streams_id = df_streams.dropna(subset=['stream_id'])
    has_churned= df_streams.groupby('customer_id').\
            aggregate({'subscription_stopped':'max'})['subscription_stopped'].\
            apply(lambda x:not x)
    
    return df_streams_id,has_churned
    

def process_dataframes(df_db, df_streams, has_churned, conn):
    """
    create the target dataset from the different data imported 
    INPUT : the customer dataframe, the stream data frame, the map of churned customers and the connection to the database
    OUTPUT : the cleaned data set as described question 4
    """
    ## YOUR CODE HERE
    query_invoice = '''
        SELECT invo.customer_id,invo.invoice_item_id,invoice_item.invoice_item
        FROM invoice AS invo
        LEFT JOIN invoice_item ON invo.invoice_item_id=invoice_item.invoice_item_id;
        
    '''
    com_df = df_db.join(pd.DataFrame(has_churned),on='customer_id')
    com_df['new_boc'] = com_df['DOB'].apply(lambda x:x.split('/')). \
                        apply(lambda x: (x[0]+'/'+x[1]+'/'+'19'+x[-1]) if int(x[-1])>20 \
                              else (x[0]+'/'+x[1]+'/'+'20'+x[-1]))
    com_df['age'] = com_df['new_boc'].apply(lambda x:datetime.datetime.now()-pd.to_datetime(x)).astype('timedelta64[Y]')
    com_df['customer_name'] = com_df['first_name']+','+com_df['last_name']
    com_df['is_subscriber'] = com_df['subscription_stopped'].apply(lambda x: not x)
    ##invoice item
    invoice = pd.read_sql_query(query_invoice,conn)
    merge_invoice = pd.merge(com_df,invoice,left_on='customer_id',right_on='customer_id')
    # ## number of streams
    count_stream = df_streams.groupby(['customer_id']).aggregate({'stream_id':'count'})
    com_stream_df = merge_invoice.join(count_stream,on='customer_id')
    com_stream_df['num_streams'] = com_stream_df['stream_id']
    com_stream_df['subscriber_type'] = com_stream_df['invoice_item'] 
    com_stream_df['country'] = com_stream_df['country_name']
    cols =['customer_id','country','age','customer_name','is_subscriber','num_streams','subscriber_type']
    
    return com_stream_df[cols]
    
    
def update_target(target_file, df_clean, overwrite=False):
    """
    write the clean data in a target file located in the working directory.
    Overwrite the existing target file if overwrite is false, otherwise append the clean data to the target file
    INPUT : the name of the target file, the cleaned dataframe and an overwrite flag
    OUPUT : None
    """
    ## YOUR CODE HERE
#     df_target = pd.read_csv(target_file)
    if overwrite:
        df_clean.to_csv(target_file,index=False)
    else:
        df_clean.to_csv(target_file,mode='a', header=False,index=False)
    
        
if __name__ == "__main__":
  
    ## collect and handle arguments with getopt or argparse
    ## YOUR CODE HERE
    DATA_DIR = os.path.join("..","data")
    parser = argparse.ArgumentParser(description='build a ETL pipeline with input sqlit database and csv file')
    parser.add_argument('input_data',  type=str, nargs='+',
                    help='database file path')
#     parser.add_argument('stream_csv',  type=str, nargs='+',
#                     help='csv file path')
#     parser.add_argument('target_file',type=str,help='output file')
#     parser.add_argument('overwrite',type=bool,help='overwrite or not')
    
    args = parser.parse_args()
    if len(args.input_data)==4:
        db_path,stream_csv,target_file,overwrite = args.input_data
    else:
        db_path,stream_csv,target_file= args.input_data
        over_write=False
    print(db_path,stream_csv,target_file,overwrite)
    ## make the connection to the database
    ## YOUR CODE HERE
    conn = connect_db(db_path)
#         ingest_stream_data(file_path)
    ## ingest the data and transform it
    ## YOUR CODE HERE
    df_db = ingest_db_data(conn)
    df_streams,has_churned = ingest_stream_data(stream_csv)
    
    ## write the transformed data to a csv
    ## YOUR CODE HERE
    df_clean = process_dataframes(df_db, df_streams, has_churned, conn)
    update_target(target_file, df_clean, overwrite=overwrite)

'''
Created on Dec 4, 2020

@author: wakana_sakashita
'''
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime
import time
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
#import psycopg2
#import pyodbc
import os
import math
import sys


#----------------------------------------------
# creating engine for Snowflake database
#----------------------------------------------
def snowflake_engine():
    
    engine = create_engine(URL(
        account = os.environ.get('snowf_account'),
        user = os.environ.get('snowf_username'),
        password = os.environ.get('snowf_password'),
        database = os.environ.get('snowf_database'),
        schema = os.environ.get('snowf_schema'),
        warehouse = os.environ.get('snowf_warehouse'),
        role = os.environ.get('snowf_role'),
    ))

    return engine


#----------------------------------------------
# writing dataframe into CSV file
#  input paramater : filname for CSV file, dataframe, the number of companies that already stored into dataframe
#----------------------------------------------
def write_toCSV(fname, temp_df):
    
    path = os.environ['localdir_path']
    
    print('---- Writing to CSV file --------')
    try:
        temp_df.to_csv(path + f'{fname}.csv', header = True)
    
    except:
        print('++++++ the error occured during CSV file generation ++++++++')
        
    return

#----------------------------------------------------
# delete all the CSV chuck files before processing
#----------------------------------------------------
def delete_chunk(name):

    c_file = os.environ['localdir_path'] + name + '*.csv'

    if os.path.exists(c_file):
        try:
            os.remove(c_file)
        except:
            print('+++++ the error occured during deleting all the CSV files +++++++++++')

    return


#-----------------------------------------------------
# splitting dataframe into chunks
#-----------------------------------------------------
def chunk(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


    
def main(args): 

    # Arguments set
    print(args)
    chunkname = args[0]
    num_thread = int(args[1])

    # Timer set
    start_main = time.perf_counter()
    print(time.strftime("%Y-%m-%d,%H:%M:%S").split(','))

    # number of threads running concurrently
#    NUMTHREAD = 10


    # preparing a query to send to XF DB
    query = '''
    SELECT distinct cc.companyId
    FROM ciqCompany cc
    JOIN ciqCompanyStatusType ccst (nolock) ON cc.companyStatusTypeId = ccst.companyStatusTypeId
    JOIN ciqCompanyType cct (nolock) ON cc.companyTypeId = cct.companyTypeId
    WHERE cc.companyTypeId IN ( 1,2,4,5 )
    AND cc.companyStatusTypeId IN (1,2)
    AND cc.CountryId in (1)
    order by cc.companyId
    '''
    # making conenction to XF DB and sending a query
    print('---- sending query to XF DB -------\n')
    engine=snowflake_engine()
    con = engine.connect()

    try:
        df_ciq = pd.read_sql(query, con)
    except Exception as e:
        print(f'++++ Error occured during sending query {e.message} +++++')
    finally:
        con.close()
        engine.dispose()
        print(f'--- Total returned rows : {len(df_ciq)} ----------------------------')


    #   clear all the CSV files if there are any
    delete_chunk(chunkname)
    
    # deciding the size of chunked dataframe
    rows = len(df_ciq)
    size = math.ceil(rows / num_thread )
    print(size)


    count = 0
    for df_ciq_chunk in chunk(df_ciq, size):
        write_toCSV(f'{chunkname}{count+1}', df_ciq_chunk)
        count += 1

    if count != num_thread :  
        print(f'+++++ failed to generate CompanyID CSV Files  -- the number of file generated : {count} ++++++++')
        sys.exit(1)


        
    print(f'------ Completed creating company IDs LIST--- the time spent: {time.perf_counter() - start_main} sec. -------')        
    sys.exit()
    

if __name__ == '__main__':
    main(sys.argv[1:])

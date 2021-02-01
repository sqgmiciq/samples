'''
Created on Dec 6, 2020

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
import sys

#---------------------------------------------------------------------------
# connecting CIQ API and get JSON response data 
# (input parameter : JSON request, being used to send via HTTPS POST)
# (returned prameter : JSON response)  
#---------------------------------------------------------------------------
def getCIQAPIdata(json):

    # API user credential
    usr = os.environ.get("api_username")
    pwd = os.environ.get("api_password")
   
    # API Direct Webservice Endpoint
    url = "https://api-ciq.marketintelligence.spglobal.com/gdsapi/rest/v3/clientservice.json"
    # required header
    headers = {'Content-Type': 'application/json'}
    
    resp = requests.post(url, auth=HTTPBasicAuth(usr, pwd), headers=headers, data=json)
    
    
    if(resp.status_code >= 400 and resp.status_code < 600):
        print("HTTP error %d is returned during the API process." %(resp.status_code))
        print(resp.text)
        return 'NOT_AVAILABLE'

    data = resp.json()

    if(isinstance(data, dict) is False):
        print(resp.text)
        return 'NOT_AVAILABLE'
    
    elif('Errors' in data.keys()):
        print(f'Error occurred during the API process : {data}')
        return 'NOT_AVAILABLE'


    return data

#----------------------------------------------
# creating engine for PostgreSQL database
#----------------------------------------------
def postgres_engine(table):
    
    user =os.environ.get('w_postgres_username')
    pw = os.environ.get('w_postgres_password')
    host = os.environ.get('w_postgres_host')
    db =os.environ.get('w_postgres_db')

    print(f'--- Connecting to PostgreSQL DB------')
    engine = create_engine(f"postgres+psycopg2://{user}:{pw}@{host}/{db}")
    

    return engine

#----------------------------------------------
# writing dataframe into CSV file
#  input paramater : filname for CSV file, dataframe, the number of companies that already stored into dataframe
#----------------------------------------------
def write_toCSV(fname, temp_df, i):
    
    path = os.environ['localdir_path']

    print('---- Writing to CSV file --------')
    try:
        if i <= REQLIMIT:
            temp_df.to_csv(path + f'{fname}.csv', header = True)
        else:
            temp_df.to_csv(path + f'{fname}.csv', mode = 'a', header = False)
    
    except:
        print('++++++ the error occured during CSV file generation ++++++++')
        
    return

#----------------------------------------------
# Writing dataframe into Snowflake database
# (input  paramater : table name, database connection, dataframe, the number of companies that already stored into dataframe)
#--------------------------------------------------
def write_toPostgres(table, engine, temp_df, i):

    try:
        if i <= REQLIMIT:   
            engine.execute(f'DROP TABLE IF EXISTS {table}')
        temp_df.to_sql(table, con = engine, if_exists = 'append', method='multi', chunksize = 50000)
    
    except Exception as e:
        print(f'++++++ the error occured during adding {i} of the ingestion into PostgreSQL table {table} +++++')
        return

    finally:
        print(f'------ {i} of the ingestion into {table} has completed (actual rows ingested : {len(temp_df)}) ----------------------')

    return

#------------------------------------------------------------
# Writing API JSON result to dataframe
# (input parameter : API JSON response)
# (output parameter : dataframe)
#------------------------------------------------------------# 
def save_toDF(data):
    
    # Create empty dataframe
    df = pd.DataFrame(columns= ['CompanyId','QC1','QC2','QC3','QC4','QC5','QC6','QC7','QC8','QC9','QC10'])
#    df = pd.DataFrame()

    inx = 0   # row number of dataframe that the data is written
    
    # Looping for each API responses in JSON response
    for r in data['GDSSDKResponse']:
        
        # if any quickcomp data is returned
        if r['NumRows'] > 0 :
            
            # write value of Identidier (without IQ prefix) into CompanyId column 
            df.loc[inx,'CompanyId'] = r['Identifier'][2:]
            
            # write the quickcomp data into QC* columns - if nultiples, arrays of data is returned. 
            # so it loops for the number of NumRows which indicates the number of arrays.
            # column names have the number as sufix.
            for i in range(0,r['NumRows']):
                df.loc[inx, 'QC'+ str(i+1)] = r['Rows'][i]['Row'][0]
            inx += 1
    
    # filtering out rows which has 'Data Unavailble' in QC1 column
    df = df.query(f'not QC1 in "Data Unavailable"')
    
    # set index on CompanyId column
    df.set_index(['CompanyId'], inplace = True)
    
    # adding 'TimeStamp' column with current date/time
    df.insert(len(df.columns),'TimeStamp', datetime.datetime.now().replace(microsecond=0))
    
    return df

#-------------------------------------------------------------------------
#  MultiProcessing main function
#--------------------------------------------------------------------------
def main(args):
    
    print(args)
    fname = args[0]
    num = args[1]

    # Read CompanyID CSV file to dataframe
    try:
        df = pd.read_csv(f'{os.environ["localdir_path"]}{fname}{num}.csv' )
    except:
        print('+++++ the error occure during loading CompanyID CSV file ++++++')
        sys.exit(1)

    # Set Temp table name for PostfreSQL 
    temp_tbl = 'temp_qc'

    # Get Connection for PostgreSQL DB
    engine = postgres_engine(os.environ["w_postgres_db"])
    
    # Timer set
    start = time.perf_counter()

    print(f'---Task{num}: started ---------------------------------')
    

    # setting up JSON request for quickcomp
    json_head = """{ "inputRequests": ["""
    json_main = ''
    json_end = "]}"

    #------------------------------------------------------------------------------------------------------------------------
    # using companyIDs obtained via query to XF DB, build JSON request for quickcomp and send to API endpoint. 
    # ** the number of requests to send per one HTTP post is variable (Max :500)
    # parsing the JSON response to dataframe and write the dataframe to CSV and/or PostfresSQL database
    #---------------------------------------------------------------------------------------------------------------
    for i in range(0,len(df['companyid'])):
    
        # when the number of API requests in JSON reaches to REQLIMIT
        if i and i % REQLIMIT == 0 : 
        
            # send JSON request to API endpoint
            js = getCIQAPIdata(json_head + json_main + json_end)
        
            # when API returns valid JSON response --> 
            #  1- storing the parsed data into dataframe, 
            #  2- write the dataframe into CSV and/or PostgreSQL database 
            if not js == 'NOT_AVAILABLE':
                df_comp = save_toDF(js)
#                write_toCSV(f'quickcomp{num}', df_comp, i)
                write_toPostgres(temp_tbl+num, engine, df_comp, i)
                # empty the JSON request
                json_main = ''
            
            else: 
                print('++++ CIQ API encountered an error.++++')
                sys.exit(1)
            
        # adding API request for a company to JSON request until it reaches to REQLIMIT
        else : 
        
            json_temp = """
            {"function": "GDSHE", "identifier": "IQ%s", "mnemonic": "IQ_QUICK_COMP", "properties":{"startRank": "1", "endRank": "10"}},
            """ %(df.iloc[i]['companyid'])
            json_main += json_temp
        

    if not json_main == '':  # processing for the last batch of API request if there are any.

        # send JSON request to API endpoint
        js = getCIQAPIdata(json_head + json_main + json_end)
    
        # when API returns valid JSON response --> 
        #  1- storing the parsed data into dataframe, 
        #  2- write the dataframe into CSV and/or PostgreSQL database 
        if not js == 'NOT_AVAILABLE':
            df_comp = save_toDF(js)
#            write_toCSV(f'quickcomp{num}', df_comp, i)
            write_toPostgres(temp_tbl+num, engine, df_comp, i)
    
    else: 
        print('++++ CIQ API encountered an error.++++')
        sys.exit(1)


    engine.dispose()
    
    print(f'------ Completed the task{num} --- the time spent: {time.perf_counter() - start} sec. -- {time.strftime("%Y-%m-%d, %H:%M:%S").split(",")} -----\n')    
    sys.exit()


if __name__ == '__main__':
    
    # concurrent mnemonic function limit
    REQLIMIT = 1500

    main(sys.argv[1:])
    
    

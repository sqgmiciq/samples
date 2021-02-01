'''
Created on Dec 10, 2020

@author: wakana_sakashita
'''
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine
import os
import sys


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


#--------------------------------------------------------------------------
def main(args):
    
    print(args)
    num = int(args[0])
    table = args[1]
    
    if num <= 1 : sys.exit

    temp_tbl = "temp_qc"

    engine = postgres_engine('quickcomp')
    
    # Timer set
    start = time.perf_counter()

    # preparing a query to send to PostgreSQL database
    query = f'CREATE TABLE {table} as TABLE {temp_tbl}1 WITH NO DATA;'

    try : 
        engine.execute(query)
    except Exception as e:
        print(f'++++ Error occured during creating a table {table} ++++++')
        engine.dispose()
        sys.exit(1)

    subq = ''
    for i in range(2,num+1):
        subq = subq + f'UNION ALL SELECT * FROM {temp_tbl}{i} '

    query = f'INSERT INTO {table} select * from {temp_tbl}1 {subq}'
    
    try :
        print(query)
        engine.execute(query)
    except Exception as e:
        print(f'++++ Error occured during the union all the temp tables ++++++++')

    

    engine.dispose()
    
    print('-----------------------------------------------------------------------------------------------------------------------------------------------------------')
    print(f'------ Completed creating table for all thee ingested quickcomp data  --- the time spent: {time.perf_counter() - start} sec. -- {time.strftime("%Y-%m-%d, %H:%M:%S").split(",")} -----')    
    print('-----------------------------------------------------------------------------------------------------------------------------------------------------------')
    sys.exit()


if __name__ == '__main__':
    
    main(sys.argv[1:])
    
    

import psycopg2.extras as extras
import psycopg2
import sys
from prefect import get_run_logger


 
param_dic_prod ={"host" : 'metastore-cluster.cluster-cpc07jv5juk1.ap-south-1.rds.amazonaws.com',
    "database" : 'analytics_prod',
    "user" : 'prefect',
    "password" : 'prefect@123'}

param_dic_dev ={"host" : 'mvsgomsdb01-instance-1.crtnm5nnvmqt.ap-south-1.rds.amazonaws.com',
    "database" : 'prefect_analytics',
    "user" : 'mk_prefect_rishav',
    "password" : 'Nc37r9G3gw9'}
   
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    logger = get_run_logger() 
    conn = None
    try:    
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error while connecting to PostgreSQL")   
        raise error           
    logger.info("Connected to postgres database server")
    return conn

def save_to_postgres_insert(df, table): 
    logger = get_run_logger()
    print(f" Preparing to insert {len(df)} rows into {table}")
    print(f" Columns: {list(df.columns)}")  
    conn = connect(param_dic_dev)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query  = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" %(table, cols) 
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
       # logger.info("Error from function save_to_postgres_insert(): %s" % error)
        print(f" Error inserting into {table}: {error}")     
        conn.rollback()    
        cursor.close()
        return 1   
    cursor.close()
    conn.close()
    
def deleteDeltaInvoiceDetailsPostgresTableChunk(df, table, chunk_size=10000):
    logger = get_run_logger() 
    conn = connect(param_dic_prod)
    cursor = conn.cursor()
    orderdtl_codes = df['Orderdtl_Code'].tolist()
    num_codes = len(orderdtl_codes)
    for i in range(0, num_codes, chunk_size):
        code_chunk = orderdtl_codes[i:i + chunk_size]
        code_chunk_str = ','.join(str(x) for x in code_chunk)
        query = f"DELETE FROM {table} WHERE Orderdtl_Code IN ({code_chunk_str})"        
        try:
            cursor.execute(query)
            conn.commit()
            logger.info("SUPERDASH Chunk number = " + str(i) + " has been Deleted")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info("Error from table "+str(table)+" deleteDeltaInvoiceDetailsPostgresTableChunk Chunk is "+ str(i) , error)
            conn.rollback()
            cursor.close()
            return 1       
    cursor.close()
    conn.close()   
    
def truncatePostgresTable(table):
    logger = get_run_logger() 
    try:     
        conn_trunc = connect(param_dic_prod)
        cursor = conn_trunc.cursor()
        cursor.execute("TRUNCATE TABLE rfmanalysis")            
        conn_trunc.commit()
        logger.info("RFMANALYSIS is truncated" )            
    except (Exception, psycopg2.Error) as error:
        logger.info("Error while Truncating table "+ table +" in Postgres" , error)
        conn_trunc.rollback()        
        print("Error while Truncating table in Postgres" , error)
    finally:     
        if cursor:
            cursor.close()
        if conn_trunc:
            conn_trunc.close() 
             
def save_to_postgres_upsert(df, table,conflict_index):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    conn = connect(param_dic_prod)
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]    
    cols = ','.join(list(df.columns))
    cols1 = ','.join(list(df.columns[1:]))
    cols2 = ','.join(list('EXCLUDED.'+df.columns[1:]))   

    # SQL quert to execute
    query = f"INSERT INTO {table}({cols}) VALUES %s ON CONFLICT ({conflict_index}) DO UPDATE SET ({cols1}) = ({cols2})"
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        #logger.info("Error: %s" % error) 
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    conn.close()    

def execute_values(conn, df, table, conflict_cols):
    '''
    Using psycopg2.extras.execute_values() to insert dataframes into psql
    '''
   
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    print(cols)
    # SQL quert to execute
    l1=list(set(df.columns)-set(conflict_cols))
    l2=list(df.columns)
    
    cols = ','.join(l2)
    cols1 = ','.join(l1)
    cols2 = ','.join(list('EXCLUDED.'+str(i) for i in l1))
    conflict_calls=",".join(conflict_cols)   
    query = f"INSERT INTO {table}({cols}) VALUES %s ON CONFLICT ({conflict_calls}) DO UPDATE SET ({cols1}) = ({cols2})"
    print('query_execute'+ query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:       
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def execute_sql_transfer(df,sql_table,conflict_cols):
    conn = connect(param_dic_prod)
    for i in range(0,len(df)):
        execute_values(conn,df[i:i+1],sql_table,conflict_cols)
    conn.close()
    
# def update_orderview(df):
#     try:     
#         conn1 = connect(param_dic)
#         cursor = conn1.cursor()     
#         for index, row in df.iterrows():
#             cursor.execute("UPDATE orderview SET Invoiceheader_code=%s	,Price=%s,DiscountPercentage=%s,Order_Status=%s,UpdatedDate=%s WHERE Invoicedtl_Code = %s",
#                 (row['Invoiceheader_code'], row['Price'], row['DiscountPercentage'], row['Order_Status'], row['UpdatedDate'], row['Invoicedtl_Code'])
#             )        
#         conn1.commit()
#     except (Exception, psycopg2.Error) as error:
#         conn1.rollback()
#         print("Error while connecting to PostgreSQL", error)
#     finally:     
#         if cursor:
#             cursor.close()
#         if conn1:
#             conn1.close() 
    
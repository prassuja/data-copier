import pandas as pd
import os
import sys

from read import get_json_reader

from write import load_db_table

def process_table(base_dir, conn, table_name):
    json_reader = get_json_reader(base_dir, table_name)
    for df in json_reader:
        load_db_table(df, conn, table_name, df.columns[0])

def main():
    #fp = 'C:\\Users\\sprasai\\Research\\data\\retail_db_json\\order_items\\part-r-00000-6b83977e-3f20-404b-9b5f-29376ab1419e'

    # dynamically reading files
    # dividing path into chunks
    # use import os

    #base_dir = 'C:\\Users\\sprasai\\Research\\data\\retail_db_json\\'
    #table_name = 'order_items\\'
    #table_name = 'departments\\'
    #f'{base_dir}{table_name}'
    #os.listdir(f'{base_dir}{table_name}')[0] #returns the first file on the folder
    #file_name = os.listdir(f'{base_dir}{table_name}')[0]

    #fp = f'{base_dir}{table_name}{file_name}'

    #print(fp)


    #df = pd.read_json(fp, lines=True)

    #print(df.count())
    #print(df.describe())

    #print(df.dtypes)

    #print(df[['order_item_order_id', 'order_item_subtotal']][:10])

    #filtering data

    #print(df[df['order_item_order_id'] == 2])

    # Reading data in chunks

    #json_reader = pd.read_json(fp, lines=True, chunksize=1000)

    #Now use for loop to read each chunk of data as dataframe

    #for idx, df in enumerate(json_reader):
    #    print(f'Number of records in chunk with index {idx} is {df.shape[0]}')

    #query_dept = 'select * from departments'

    #conn = 'postgresql://retail_user:1Ae2a42c@localhost:5452/retail_db'

    #df = pd.read_sql(query, conn)

    #print(df)

    #users_list = [
    #    {'user_first_name': 'Scott', 'user_last_name': 'Tiger'},
    #    {'user_first_name': 'Donald', 'user_last_name': 'Duck'}
    #]

    #df = pd.DataFrame(users_list)

    #df.to_sql('users', conn, if_exists='append', index=False)

    #print(pd.read_sql(query, conn))

    #df = pd.read_json(fp, lines=True)

    #table_named = 'departments'

    #df.to_sql(table_named, conn, if_exists='append', index=False)

    #print(pd.read_sql(query_dept, conn))

    #Writing data in chunks to the database

    #table_order = 'orders\\'

    #table_named_order = 'orders'

    #file_order = os.listdir(f'{base_dir}{table_order}')[0]

    #fp_order = f'{base_dir}{table_order}{file_order}'

    #json_reader = pd.read_json(fp_order, lines=True, chunksize=1000)

    #for df in json_reader:
    #    min_key = df['order_id'].min()
    #    max_key = df['order_id'].max()
    #    df.to_sql(table_named_order, conn, if_exists='append', index=False)
    #    print(f'processed {table_named_order} with in the range of {min_key} and {max_key}')

    #query_orders = 'Select * from orders limit 10;'

    #print(pd.read_sql(query_orders, conn))

    #*******************************************************************
    #Importing from read file and write file



    base_dir = os.environ.get('base_dir')
    #table_name = os.environ.get('table_name')
    #for multiple tables
    table_names = sys.argv[1].split(',') #set arguments in the edit configuration under run menu

    # conn = 'postgresql://retail_user:1Ae2a42c@localhost:5452/retail_db'
    configs = dict(os.environ.items())
    conn = f'postgresql://{configs["DB_USER"]}:{configs["DB_PASS"]}@{configs["DB_HOST"]}:{configs["DB_PORT"]}/{configs["DB_NAME"]}'
    for table_name in table_names:
        process_table(base_dir, conn, table_name)

if __name__ == "__main__":
    main()
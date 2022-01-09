import pandas as pd
import os

def main():
    #fp = 'C:\\Users\\sprasai\\Research\\data\\retail_db_json\\order_items\\part-r-00000-6b83977e-3f20-404b-9b5f-29376ab1419e'

    # dynamically reading files
    # dividing path into chunks
    # use import os

    base_dir = 'C:\\Users\\sprasai\\Research\\data\\retail_db_json\\'
    table_name = 'order_items\\'
    f'{base_dir}{table_name}'
    #os.listdir(f'{base_dir}{table_name}')[0] #returns the first file on the folder
    file_name = os.listdir(f'{base_dir}{table_name}')[0]

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

    query = 'select * from users'

    conn = 'postgresql://retail_user:1Ae2a42c@localhost:5452/retail_db'

    df = pd.read_sql(query, conn)

    print(df)



if __name__ == "__main__":
    main()
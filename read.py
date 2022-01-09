#if __name__ == '__main--':
#    print('Hello From Read File')

#def dummy():
#   print('Hello From Dummy function on Read file')

import os
import pandas as pd

def get_json_reader(base_dir, table_name, chunksize=1000):

    file_name = os.listdir(f'{base_dir}\\{table_name}')[0]
    fp = f'{base_dir}\\{table_name}\\file_name'

    return pd.read_json(fp, lines=True, chunksize=chunksize)

if __name__ == '__main__':
    base_dir = os.environ.get('base_dir') # would be same as above and set as environment variables
    table_name = os.environ.get('table_name') # same as base_dir
    json_reader = get_json_reader(base_dir, table_name)
    for idx, df in enumerate(json_reader):
        print(f'Number of records in chunk with index {idx} is {df.shape[0]}')


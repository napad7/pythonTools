'''

1. create file - PY/Data/bank1.txt - done
2. pandas update - add padding 0 - done
3. pandas update - julian date conversion - done
4. pandas update - account number int - done
5. pandas update - remove data if yy is less than certain number - done
6. pandas update - add column with pro - done
7. creat a function for whole thing
8. append pandas based on file name
9. create a seperate df if file name different
10. monitor the folder

'''

from os import chdir
from os import getcwd
from os import listdir
from glob import glob
import pandas as pd
import numpy as np


print('home')

df_pro1 = None
df_pro2 = None
df_pro3 = None
df_pro4 = None
df_pro5 = None
df_discard = None
final_df = None
all_pros = []

pro_1 = ['001', '021']
pro_2 = ['802']
pro_3 = ['501', '076']

def single_file(folder):
    # list_of_files = listdir(getcwd())
    for f in listdir(getcwd()):
        if f.endswith('.txt'):
            return f


def convert_file_to_df(file):
    colnames = ['bankId', 'accountNo', 'julianDate']
    df = pd.read_csv(file, names=colnames, header=None, converters={'bankId': '{:0>3}'.format})
    return df

def convert_account_no_to_int(df):
    df['accountNo'] = df['accountNo'].astype('int')
    return df


def convert_julian_to_yymm(df, ex_year):
    df['julianDate'] = df['julianDate'].astype('str')
    df['year'] = df['julianDate'].str[:2]
    df['year'] = df['year'].astype(int)
    df = df.drop(df[df['year'] < ex_year].index)
    df['year'] = df['year'].astype(str)
    df['day'] = df['julianDate'].str[2:]
    df['month'] = df['day'].astype(int)
    df['month'] = df['month'] // 30
    df['month'] = df['month'].astype(str).str.zfill(2)
    df['yymm'] = df['year'] + df['month']
    df['yymm'] = df['yymm'].astype(int)
    return df

df convert_julian_to_yymm_int_way(df, ex_year):
    df['julianDate'] = pd.to_numeric(df['julianDate'], errors = ignore)
    df['year'] = df['julianDate'] // 1000
    


def drop_columns(df):
    final_df = df.drop(['julianDate', 'year', 'day', 'month'], axis=1)
    return final_df

def add_pro_column(df):
    final_df = df.assign(pro=np.where(df['bankId'] == '001', 'pro1', 'NA'))
    return final_df


def split_dataframe_to_chunks(df, n):
    df_len = len(df)
    count = 0
    dfs = []

    while True:
        if count > df_len-1:
            break

        start = count
        count += n
        # print('{0} : {1}'.format(start, count))
        dfs.append(df.iloc[start : count])
    return dfs


def shuffle_df(df):
    return df.reindex(np.random.permutation(df.index))

folder_location = '/home/random1/Documents/PY/Scripts/data_work/file_data/'
print('main folder location:{}'.format(folder_location))

getcwd()
print('current working directory: {}'.format(getcwd()))
chdir(folder_location)
current_directory = getcwd()
print('new working directory: {}'.format(current_directory))

# all_pros = []

for f in listdir(getcwd()):
    # print('for loop {}'.format(i))
    if f.endswith('.txt'):
        # print('file if loop {}'.format(i))
        df = convert_file_to_df(f)
        convert_account_no_to_int(df)
        dfs = split_dataframe_to_chunks(df, 500)

        

        # loop = 1
        for df in dfs:
            df_data = convert_julian_to_yymm(df, 20)
            small_chunck_df = drop_columns(df_data)
            final_df = pd.concat([final_df, small_chunck_df])
            print('every time')
            print(final_df.info())
            # print('inside for loop {0} length of final df: {1}'.format(loop, len(final_df)))
            # print('bank ID inside loop {}'.format(final_df.bankId.unique()))
            # loop += 1
        
        bankId = final_df.bankId.unique()
        print('bank id in final df: {}'.format(bankId))
        print('length of final df: {}'.format(len(final_df)))
        
        if bankId in pro_1:
            print('pro 1 information')
            df_pro1 = pd.concat([df_pro1, final_df])
            df_pro1 = df_pro1.sample(frac=1).reset_index(drop=True)
            print(df_pro1.info())
        elif bankId in pro_2:
            # print('pro 2 information')
            df_pro2 =  pd.concat([df_pro2, final_df])
            df_pro2 = df_pro2.sample(frac=1).reset_index(drop=True)
            print(df_pro2.info())
        else:
            print('pro 3 information')
            df_pro3 = pd.concat([df_pro3, final_df])
            df_pro3 = df_pro3.sample(frac=1).reset_index(drop=True)
            # print(df_pro3.info())
        final_df = None


all_pros.extend([df_pro1, df_pro2, df_pro3])

# for pros in all_pros:
#     print(pros.info())
name_pro = 1
name_list = 0

for pros in all_pros:
    pro_length = len(pros.index)
    print('{}, {}'.format(type(pro_length), pro_length))
    dfs = split_dataframe_to_chunks(pros, pro_length//4)

    names = ['sdp_node1','sdp_node2','pos_node1','pos_node2', 'discard']
    for df in dfs:
        file_name = '{0}_pro{1}'.format(names[name_list], name_pro)
        # file_name = '{}'.format(i)
        df.to_csv('{}.txt'.format(file_name), index=False)
        name_list += 1
    name_pro += 1
    name_list = 0

# final_df.to_csv('~/Documents/PY/Scripts/data_work/final.txt', index=False)
# print(df.info())

# df_new = pd.concat([df_a, df_b])

# print(df['bankId'].unique())

# print(df.index)
# print(df.head)
# print(df.dtypes)
# print(final_df.head())

# print(final_df.dtypes)




# f = '~/Documents/PY/Scripts/data_work/bank1.txt'
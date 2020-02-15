import pandas as pd

mvc_df = pd.read_csv('C:/Users/Gideon/Desktop/RTAI/dataset/mvc.csv')
lookup_df = pd.read_csv('C:/Users/Gideon/Desktop/RTAI/dataset/lookup.csv')

filter_list = [11,12,13,14,20,19,17]
filter_df = lookup_df[lookup_df['LocationID'].isin(filter_list)]
filter_df = filter_df[['LocationID', 'Borough', 'Zone']]

print(filter_df)
print(lookup_df.shape, filter_df.shape)
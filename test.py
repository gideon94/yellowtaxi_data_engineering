import pandas as pd
import datetime

mvc_df = pd.read_csv('C:/Users/Gideon/Desktop/RTAI/dataset/mvc.csv')
# lookup_df = pd.read_csv('C:/Users/Gideon/Desktop/RTAI/dataset/lookup.csv')

# filter_list = [11,12,13,14,20,19,17]

# filter_df = lookup_df[lookup_df['LocationID'].isin(filter_list)]
# filter_df = filter_df[['Borough','Zone']]
# print(filter_df.to_json(orient='records'))
times = ['01-01-2018 00:21:00','01-01-2018 01:21:00']
times_from = pd.to_datetime(times)
times_to = times_from + pd.DateOffset(hours=1)     
mvc_df['DATE'] = pd.to_datetime(mvc_df['CRASH DATE'] + ' ' + mvc_df['CRASH TIME'])

mvc_df = mvc_df['DATE'].loc[times_from[0]:times_to[0]]
print(mvc_df.head())


# mvc_df = mvc_df.loc[mvc_df['DATE']> times_from & mvc_df['DATE']< times_to]

# print(mvc_df.head())


# for t in times:
#      date = datetime.datetime.strptime(t, '%d-%m-%Y %H:%M:%S')
#      date_time_obj_ = date_time_obj + datetime.timedelta(hours = 1)
#      d = '{:%d-%m-%Y %H:00:00}'.format(date_time_obj)
#      d_ = '{:%d-%m-%Y %H:00:00}'.format(date_time_obj_)
#      date.append(d)
#      date_.append(d_)

# print(date,date_)
# filter_mvc = mvc_df[mvc_df['DATE'].between(date,date_)]

# # date = []
# # date_ = []
# # time = []
# # time_ = []
# # for t in times:
# #     date_time_obj = datetime.datetime.strptime(t, '%d-%m-%Y %H:%M:%S')
# #     date_time_obj_ = date_time_obj + datetime.timedelta(hours = 1)
# #     d_ = '{:%d-%m-%Y}'.format(date_time_obj_)
# #     d = '{:%d-%m-%Y}'.format(date_time_obj)
# #     t='{:%H:%M}'.format(date_time_obj)
# #     t_='{:%H:%M}'.format(date_time_obj_) 
# #     date.append(d)
# #     date_.append(d_)
# #     time.append(t)
# #     time_.append(t_)
# # print(date, date_,time,time_)

# # mvc_df['CRASH DATE'] = pd.to_datetime(mvc_df['CRASH DATE'])
# # mvc_df['CRASH TIME'] = pd.to_datetime(mvc_df['CRASH TIME'])
# # filter_mvc = mvc_df[mvc_df['CRASH DATE'].isin(date)]
# # filter_mvc = filter_mvc.loc[filter_mvc['CRASH TIME']> time[0] & filter_mvc['CRASH TIME']< time_[0]]
# # print(filter_mvc)
# # mvc_df['CRASH DATE'] = pd.to_datetime(mvc_df['CRASH DATE'])
# # filter_mvc = mvc_df[mvc_df['CRASH DATE'].isin(date)]
# # filter_mvc = filter_mvc[filter_mvc['CRASH TIME'].between(time[0],time_[0])]
# # print(filter_mvc)

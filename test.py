import datetime
from collections import defaultdict
from collections import Counter

freq_daily = defaultdict(int)
freq_hourly = defaultdict(int)
date_time_obj = datetime.datetime.strptime('01-01-2018  00:21:05', '%d-%m-%Y %H:%M:%S')
hour,day = date_time_obj.time().hour,date_time_obj.date().day
freq_hourly[hour] = freq_hourly[hour]+1
freq_daily[day] = freq_hourly[day]+1
msg = {'daily': freq_daily, 'hour' :freq_hourly}
print(msg)

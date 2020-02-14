######Not Needed#######
# import json
# import stomp
# import datetime
# import pandas as pd
# from collections import defaultdict
# from collections import Counter
# from .listener import Listener
# from .publisher import Publisher
# from .subscriber import Subscriber

# def publish_frequency(msg):
#     publisher=Publisher()
#     publisher.publish(json.dumps(msg), '/queue/analytics/peaktime')
#     print(msg)


# def action(message):
#     freq_daily = defaultdict(int)
#     freq_hourly = defaultdict(int)
#     date_time_obj = datetime.datetime.strptime(message['tpep_pickup_datetime'], '%d-%m-%Y %H:%M:%S')
#     hour,day = date_time_obj.time().hour,date_time_obj.date().day
#     freq_hourly[hour] = freq_hourly[hour]+1
#     freq_daily[day] = freq_hourly[day]+1
#     freq = {'daily': freq_daily, 'hour' :freq_hourly}

# def main():
#     subscription=Subscriber()
#     subscription.subscribe('/queue/preprocess/cleanup', 1, Listener(subscription,action))

# if __name__ == '__main__':
#     main()
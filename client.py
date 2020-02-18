import datetime
import json
from multiprocessing import Process, Lock
from listener import Listener
from subscriber import Subscriber

exit_count=0

class ClientListener(Listener):
    def on_message(self, headers, message):
        if 'exit' in message:
            self.subscriber.disconnect()
        lock = Lock()
        p=Process(target=self.action, args=(json.loads(message),lock))
        p.start()
        p.join()

def hour_action(message,lock):
    global exit_count
    if message=='exit':
        exit_count+=1
        return
    lock.acquire()
    #print in required format
    print('***************************************')
    print('Hourly report :')
    print(message['time'])
    for loc in message['peak_zones']:
        print('Borough : ' + loc['Borough']+','+ loc['Zone'])
        print('taxis : ' + str(message['freq']))
    lock.release()
    return

def day_action(message,lock):
    global exit_count
    #print in required format
    if message=='exit':
        exit_count+=1
        return
    lock.acquire()
    print('***************************************')
    print('Daily report :')
    for time in message['peak_times']:
        start=datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        end=start + datetime.timedelta(hours=1)
        end='{:%Y-%m-%d %H:00:00}'.format(end)
        print(start+' - '+end+'   '+ 'taxis : ' + str(message['trips']))
    lock.release()
    return

def acch_action(message,lock):
    #print in required format
    if message=='exit':
        exit_count+=1
        return
    lock.acquire()
    print('***************************************')
    print('Accidents hourly report :')
    for acc in message['accident_report']:
        print(acc['CRASH DATE'])
        print(acc['CRASH TIME'])
        print(acc['BOROUGH'])
        print('Reason:')
        print(acc['CONTRIBUTING FACTOR VEHICLE 1'])
    lock.release()
    return

def accd_action(message,lock):
    global exit_count
    if message=='exit':
        exit_count+=1
        return
    #print in required format
    lock.acquire()
    print('***************************************')
    print('Accidents daily report :')
    for acc in message['accident_report']:
        print(acc['CRASH DATE'])
        print(acc['CRASH TIME'])
        print(acc['BOROUGH'])
        print('Reason:')
        print(acc['CONTRIBUTING FACTOR VEHICLE 1'])
    lock.release()
    return

def weather_action(message,lock):
    #print in required format
    if message=='exit':
        return
    lock.acquire()
    print('***************************************')
    print('Weather hourly report :')
    print(message['weather'][0]['main']+'  '+message['weather'][0]['description'])
    print('Temperature:' +str(message['main']['temp']) +'F' + ' Feels like:' + str(message['main']['feels_like']) +'F')
    print('Wind speed:'+ str(message['wind']['speed']) + 'mph')
    lock.release()
    return

def main():
    hour_subscription=Subscriber()
    day_subscription=Subscriber()
    acc_subscription_hour=Subscriber()
    acc_subscription_day=Subscriber()
    weather_subscription=Subscriber()
    #final report after join
    
    hour_subscription.subscribe('/queue/report/hour', 'report_hour', ClientListener(hour_subscription,hour_action))
    day_subscription.subscribe('/topic/report/day', 'report_day', ClientListener(day_subscription,day_action))
    acc_subscription_hour.subscribe('/queue/report/hour_accident', 'accident_hour', ClientListener(acc_subscription_hour,acch_action))
    acc_subscription_day.subscribe('/queue/report/day_accident', 'accident_day', ClientListener(acc_subscription_day,accd_action))
    weather_subscription.subscribe('/queue/report/weather', 'weather_', ClientListener(weather_subscription,weather_action))

    while not exit_count==4:
        pass
if __name__ == '__main__':
    main()
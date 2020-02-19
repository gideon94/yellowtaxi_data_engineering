#!/usr/bin/env python
from constants import TRIPDATA_PATH 
import csv
import json
from publisher import Publisher
import stomp
from multiprocessing.pool import ThreadPool as Pool

def publish_line(args):
    (line,publisher,destination)=args
    publisher.publish(json.dumps(line),destination)

# def publish_line(tup):
#     print('inside')
#     print(tup)
#     tup[2].publish(json.dumps(tup[0]),tup[3])

def main():
    publisher1 = Publisher()
    publisher2 = Publisher()
    publisher3 = Publisher()
    publisher4 = Publisher()

    def read_line():
        with open( TRIPDATA_PATH, "r" ) as trip_data:
            reader = csv.DictReader(trip_data)
            for line in reader:
                yield line
    gen=read_line()
    pool= Pool(processes=4)
    # for line in gen:
    #     # task.map(publish_line, (line,publisher1,'/queue/source1'))
    #     # task.map(publish_line, (next(gen),publisher2,'/queue/source2'))
    #     # task.map(publish_line, (next(gen),publisher3,'/queue/source3'))
    #     # task.map(publish_line, (next(gen),publisher4,'/queue/source4'))
    #     task.starmap(publish_line, zip(line,publisher1,'/queue/source1'))
    #     task.starmap(publish_line, zip(next(gen),publisher2,'/queue/source2'))
    #     task.starmap(publish_line, zip(next(gen),publisher3,'/queue/source3'))
    #     task.starmap(publish_line, zip(next(gen),publisher4,'/queue/source4'))
    # task.close()
    # task.join()
    while True:
        try:
            pool.map(publish_line, [(next(gen),publisher1,'/queue/source1')])
            pool.map(publish_line, [(next(gen),publisher2,'/queue/source2')])
            pool.map(publish_line, [(next(gen),publisher3,'/queue/source3')])
            pool.map(publish_line, [(next(gen),publisher4,'/queue/source4')])
        except StopIteration:
            break
    pool.close()
    pool.join()
    publisher1.disconnect()
    publisher2.disconnect()
    publisher3.disconnect()
    publisher4.disconnect()

if __name__ == '__main__':
    main()

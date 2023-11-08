#!/usr/bin/env python

import sys, json, datetime, calendar, time, socket, httpx
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from threading import Thread

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()


    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    def power_consumer(name, house_range):
        consumer = Consumer(
            {
                'bootstrap.servers': '172.16.250.13, 172.16.250.14', 
                'group.id': name, 
                'auto.offset.reset': 'earliest'
            }
        )
        consumer.subscribe(["power"], on_assign=reset_offset)

        try:
            while True:
                try:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        print("Power Waiting...")
                        continue
                    elif msg.error():
                        print(msg.error())
                        print("ERROR: %s".format(msg.error()))
                    else:
                        value = json.loads(msg.value())
                        house_id = int(value.get("house_id"))

                        if house_id not in range(*house_range):
                            continue

                        power = value.get("kwh")
                        timestamp = value.get("timestamp", 0)
                        
                        housedata = f'house_id="{house_id}"'
                        try:
                            time.sleep(0.1)
                            httpx.post(f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance=RAPPERDAPPER", data=f'superpower{{{housedata}}} {float(power)} {timestamp}\n')
                        except Exception:
                            pass
                except:
                    pass
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    
    def water_consumer(name, house_range):
        consumer = Consumer(
            {
                'bootstrap.servers': '172.16.250.13, 172.16.250.14', 
                'group.id': name, 
                'auto.offset.reset': 'earliest'
            }
        )
        consumer.subscribe(["water"], on_assign=reset_offset)

        try:
            while True:
                try:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        print("Water Waiting...")
                        continue
                    elif msg.error():
                        print(msg.error())
                        print("ERROR: %s".format(msg.error()))
                    else:
                        value = json.loads(msg.value())
                        house_id = int(value.get("house_id"))
                        if house_id not in range(*house_range):
                            continue
                        power = value.get("m3")
                        timestamp = value.get("timestamp", 0)
                        
                        housedata = f'house_id="{house_id}"'
                        try:
                            httpx.post(f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance=RAPPERDAPPER", data=f'superwater{{{housedata}}} {float(power)} {timestamp}\n')
                        except Exception:
                            pass
                except:
                    pass
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    
    def heat_consumer(name, house_range):
        consumer = Consumer(
            {
                'bootstrap.servers': '172.16.250.13, 172.16.250.14', 
                'group.id': name, 
                'auto.offset.reset': 'earliest'
            }
        )
        consumer.subscribe(["heat"], on_assign=reset_offset)

        try:
            while True:
                try:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        print("Heat Waiting...")
                        continue
                    elif msg.error():
                        print(msg.error())
                        print("ERROR: %s".format(msg.error()))
                    else:
                        value = json.loads(msg.value())
                        house_id = int(value.get("house_id"))
                        if house_id not in range(*house_range):
                            continue
                        power = value.get("kwh")
                        timestamp = value.get("timestamp", 0)
                        
                        housedata = f'house_id="{house_id}"'
                        try:
                            httpx.post(f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance=RAPPERDAPPER", data=f'superheat{{{housedata}}} {float(power)} {timestamp}\n')
                        except Exception:
                            pass
                except:
                    pass
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    threads = []
    threads.append(Thread(target=power_consumer, args=("PowerThread1", (1,50))))
    threads.append(Thread(target=power_consumer, args=("PowerThread2", (51, 100))))
    threads.append(Thread(target=water_consumer, args=("WaterThread1", (1,50))))
    threads.append(Thread(target=water_consumer, args=("WaterThread2", (51,100))))
    threads.append(Thread(target=heat_consumer, args=("HeatThread1", (1,50))))
    threads.append(Thread(target=heat_consumer, args=("HeatThread2", (51,100))))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
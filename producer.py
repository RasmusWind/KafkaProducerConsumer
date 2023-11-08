#!/usr/bin/env python
import sys
import random
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import time, json
from threading import Thread
from datetime import datetime

producer = Producer(
    {'bootstrap.servers': '172.16.250.13, 172.16.250.14'}
)

def recursive_producer(topic, data, key):
    try:
        producer.produce(topic, data, key)
        producer.flush()
    except:
        print("Queue full, waiting 1 second . . .")
        time.sleep(1000)
        producer.flush()
        recursive_producer(data)

topics = {"power":[], "water":[], "heat":[]}
adult_power_consumption_range = (5, 15)
child_power_consumption_range = (4, 14)
electric_car_power_consumption_range = (24, 96)
adult_water_consumtion_range = (100, 180)
child_water_consumtion_range = (80, 180)
average_house_size = 110
average_daily_electric_car_use = 15

start_timestamp = 1262300400
today = int(time.time())

monthly_variables = {
    "1":(1.25, 1.75),
    "2":(1.25, 1.75),
    "3":(1.10, 1.60),
    "4":(1.05, 1.50),
    "5":(1.00, 1.40),
    "6":(1.00, 1.25),
    "7":(0.90, 1.15),
    "8":(0.80, 1.10),
    "9":(1.00, 1.30),
    "10":(1.10, 1.45),
    "11":(1.15, 1.55),
    "12":(1.25, 1.65),
}

def task(thread_name, house):
    old_time = start_timestamp
    
    house_id = house["id"]
    children = house["no_children"]
    adults = house["no_adults"]
    house_size = house["house_size_m2"]
    electric_cars = house["no_electric_cars"]

    heat_multipler = house_size / average_house_size


    while old_time < today:
        month = datetime.fromtimestamp(old_time).month
        multipler_range = monthly_variables.get(str(month))
        multipler = random.uniform(*multipler_range)
        adult_power_consumption = adults * random.randrange(*adult_power_consumption_range)
        children_power_consumption = children * random.randrange(*child_power_consumption_range)

        adult_water_consumtion = adults * random.randrange(*adult_water_consumtion_range)
        child_water_consumtion = children * random.randrange(*child_water_consumtion_range)
        if not multipler_range:
            continue

        electric_car_consumption = electric_cars * average_daily_electric_car_use

        heat_consumption = (((2 * house_size) * heat_multipler) * multipler) / 24 # 0 is temp value

        power_kwh = ((adult_power_consumption+children_power_consumption+electric_car_consumption) * multipler) / 24
        water_m3 = ((adult_water_consumtion+child_water_consumtion) * multipler) / 24

        recursive_producer("power", json.dumps({"house_id": house_id, "timestamp":old_time, "kwh":power_kwh}), f'{old_time}-{house_id}')
        recursive_producer("water", json.dumps({"house_id": house_id, "timestamp":old_time, "m3":water_m3}), f'{old_time}-{house_id}')
        recursive_producer("heat", json.dumps({"house_id": house_id, "timestamp":old_time, "kwh":heat_consumption}), f'{old_time}-{house_id}')
        old_time += 3600

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('data_file', type=FileType('r'))
    args = parser.parse_args()

    # Create Producer instance

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    threads = []
    with open(f'./{args.data_file.name}', "r") as f:
        houses = json.load(f)
        for i, house in enumerate(houses):
            threads.append(Thread(target=task, args=(f"Thread-{i+1}", house)))
        
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # with open("./power_data.json", "w+") as f:
    #     f.writelines(json.dumps(topics["power"]))
    # with open("./water_data.json", "w+") as f:
    #     f.writelines(json.dumps(topics["water"]))
    # with open("./heat_data.json", "w+") as f:
    #     f.writelines(json.dumps(topics["heat"]))

    # count = 0
    # for _ in range(10):

    #     user_id = choice(user_ids)
    #     product = choice(products)
    # for topic, value in topics.items():
    #     for data in value:
    #         # for key, keyvalue in data.items():
    #         recursive_producer(data)
    # #     count += 1

    # # Block until the messages are sent.
    # producer.flush()

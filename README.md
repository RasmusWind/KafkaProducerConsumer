# KafkaProducerConsumer
Kafka Producer & Consumer.

Producer takes the houses.json file as argument: ./producer houses.json
Produer will start a thread per house and start generating data.

Consumer polls the kafka cluster to get data, then sends it to victoriametrics: ./consumer.py
Consumer will start 6 threads in total. 2 threads per metric: Power, Water, Heat.
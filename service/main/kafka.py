import warnings
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaError,KafkaException
import pandas as pd
import json
import socket
from send_bot import handleML
import asyncio
from telegram import Bot
import asyncio
from elasticsearch import Elasticsearch

warnings.filterwarnings('ignore') 

# def publish_to_kafka(producer_config, topic, df):
#     # Create Kafka producer instance
#     producer = Producer(producer_config)
#     print(producer)
    
#     # Convert DataFrame to JSON and produce to Kafka topic
#     for index, row in df.iterrows():
#         # print(row)
#         json_data = json.dumps(row.to_dict())
#         # print(json_data)
#         producer.produce(topic, key=str(index), value=json_data)

#     # Wait for any outstanding messages to be delivered and delivery reports received
#     producer.flush()

def get_streaming_data(consumer_config, topic):
     
    es = Elasticsearch(
        "http://34.143.255.36:9200/",basic_auth=("elastic","elastic2023"))
    bot = Bot(token='6845513846:AAHbmWW4Hhc35lnpIwTKW63o5RiZzA3-wSQ')
    # Create Kafka consumer instance
    consumer = Consumer(consumer_config)

    # Subscribe to Kafka topic
    consumer.subscribe([topic])

    # Read and load DataFrame from Kafka
    df_received = pd.DataFrame()
    x = 0
    y = 0
    loop = asyncio.get_event_loop()

    try:
        while True:
            # if y == 10:
            #     print(y, 'messages returned with None value. Stopping consumer.')
            #     break
            msg = consumer.poll(1.0)  # Timeout in seconds
            # if msg is None:
            #     y += 1
            #     # print(msg)
            #     print(y, 'messages with None value.')
            #     continue
            # if msg.error():
            #     if msg.error().code() == KafkaError._PARTITION_EOF:
            #         continue
            #     else:
            #         print(msg.error())
            #         break

            # Deserialize JSON message and append to DataFrame
            # json_data = json.loads(msg.value())
            # # print(json_data)
            # df_received = pd.concat([df_received, pd.DataFrame.from_dict([json_data], orient='columns')])
            
            # x += 1
            # y = 0
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                print(f"Error: {msg.error()}")
                break
                

            # Process the received message
            # Deserialize JSON message and append to DataFrame
            json_data = json.loads(msg.value())
            # print(json_data)
            df_received = pd.concat([df_received, pd.DataFrame.from_dict([json_data], orient='columns')])
            # handleML(df_received)  
            loop.run_until_complete( handleML(es,bot))     
    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

    # Display the received DataFrame
    print("Received DataFrame:")
    # print(df_received)
    # return df_received



            
def main():
    # Sample DataFrame
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35],
            'City': ['New York', 'San Francisco', 'Los Angeles']}

    df = pd.DataFrame(data)

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()
    }
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'kafka-consumer',
        'auto.offset.reset': 'earliest'  # You can change this to 'latest' if you want to start reading from the latest offset
    }
    
    topic = 'group16_topic'
    # publish_to_kafka(producer_config, topic, df)
    get_streaming_data(consumer_config, topic)

if __name__ == '__main__':
    main()
    

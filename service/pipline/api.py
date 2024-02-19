from locale import normalize
from typing import Sequence
from flask import Flask, render_template, url_for, request, jsonify, make_response
import sys
import requests
import uuid
from threading import Thread
# from ..main.kafka import publish_to_kafka
import pandas as pd
import socket
from confluent_kafka import Producer
import json
from elasticsearch import Elasticsearch


app = Flask(__name__)


def clean_data(tx,wallet):
    df = pd.DataFrame(tx)
    df['address'] = df[['from_address', 'to_address']].apply(lambda x: x.min(), axis=1)
    df['num_transactions'] = df.groupby('address')['id'].transform('count')
    df['balance'] = df.groupby('address')['value'].transform('sum')/ 10**19
    df = df.drop(columns=['from_address', 'to_address', 'value', 'id','block_timestamp'])
    df = df.drop_duplicates()
    # print(df)
    return df

def index_document(es, index_name,df):
    # data = df.to_dict(orient = 'list') 
    for index, row in df.iterrows():
        data = row.to_dict()
        data['Time'] = index
        resp = es.index(index=index_name, id=index, document=data)

def publish_to_kafka(producer_config, topic, df):
    # Create Kafka producer instance
    producer = Producer(producer_config)
    print(producer)
    
    # Convert DataFrame to JSON and produce to Kafka topic
    for index, row in df.iterrows():
        # print(row)
        json_data = json.dumps(row.to_dict())
        # print(json_data)
        producer.produce(topic, key=str(index), value=json_data)

    # Wait for any outstanding messages to be delivered and delivery reports received
    producer.flush()

@app.route('/', methods=['POST'])
def post():
            # Sample DataFrame
    data = request.get_json()
    tx = pd.json_normalize(data)
    df = clean_data(tx, None)
    print(df)
    index_document(es,"it5384_group5_problem5_index",df)

    
    df = pd.DataFrame(data)

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()
    }

    topic = 'group16_topic'
    publish_to_kafka(producer_config, topic, df)
    
    return "Done"
    

@app.route('/hello', methods=['GET'])
def check_get2():
        return 'Hello world 1'

if __name__ == '__main__':
        es = Elasticsearch(
            "http://34.143.255.36:9200/",basic_auth=("elastic","elastic2023"))
        # --
        host = "0.0.0.0"
        port = 3692
        app.run(host=host, port=port, debug=True)
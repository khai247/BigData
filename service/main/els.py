from elasticsearch import Elasticsearch
import pandas as pd
def initialize_elasticsearch(url, user_name, pwd):
    es = Elasticsearch(
        url,  # Elasticsearch endpoint
        basic_auth=(user_name, pwd),  # API key ID and secret
    )
    return es

def clean_data(tx,wallet):
    df = pd.DataFrame(tx)
    df['address'] = df[['from_address', 'to_address']].apply(lambda x: x.min(), axis=1)
    df['num_transactions'] = df.groupby('address')['id'].transform('count')
    df['balance'] = df.groupby('address')['value'].transform('sum')/ 10**19
    df = df.drop(columns=['from_address', 'to_address', 'value', 'id','block_timestamp'])
    df = df.drop_duplicates()
    print(df)
    return df

def index_document(es, index_name,df):
    # data = df.to_dict(orient = 'list') 
    for index, row in df.iterrows():
        data = row.to_dict()
        data['Time'] = index
        # print(data)
        # json_data = json.dumps(data)
        # document['Time'] = df_test.index[i]
        resp = es.index(index=index_name, id=index, document=data)

    # print(resp['result'])

if __name__ == '__main__':
    es = initialize_elasticsearch("http://34.143.255.36:9200/","elastic","elastic2023")
    tx = pd.read_json('../../crawl/data/ethereum_blockchain_etl_transactions.json')
    wallet = pd.read_json('../../crawl/data/ethereum_blockchain_etl_wallets.json')
    df = clean_data(tx, wallet)
    index_document(es,"it5384_group8_problem5_index",df)

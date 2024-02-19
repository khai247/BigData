from elasticsearch import Elasticsearch



if __name__ == '__main__':
    url = "http://34.143.255.36:9200/"
    user_name = 'elastic'
    password = 'elastic2023'
    es = Elasticsearch(
        url,  # Elasticsearch endpoint
        basic_auth=(user_name, password),  # API key ID and secret
    )
    index_name = "it5384_group5_problem5_index"

    settings = {
        "number_of_shards": 2,
        "number_of_replicas": 1
    }
    mappings = {
        "dynamic": "true",
        "numeric_detection": "true",
        "_source": {
        "enabled": "true"
        },
        "properties": {
        "p_text": {
            "type": "text"
        },
        "p_vector": {
            "type": "dense_vector",
            "dims": 768
        },
        }
    }
    # Create an index
    es.indices.create(index=index_name, ignore=400, settings=settings, mappings=mappings)
    print(es.info())
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from telegram import Bot
import asyncio
from elasticsearch import Elasticsearch
import asyncio


# handle ML
async def handleML(es,bot):
    # chanel tele
    channel_id = '@group8_bigdata'

    
    index_name = 'it5384_group8_problem5_index'
    query = {"query": {"match_all": {}}}

    result = es.search(index=index_name, body=query,size=100, scroll="2m")

    hits = result.get("hits", {}).get("hits", [])
    result_query = [hit["_source"] for hit in hits]
    for document in result_query:
        print(document,5)

    raw_df = pd.json_normalize(result_query)
    df = raw_df.drop(['Time',"address"],axis=1)
    features = np.array(df)
    scaler = StandardScaler()
    features_normalized = scaler.fit_transform(features)
    # Define the Isolation Forest model.
    # Adjust the 'n_estimators' and 'contamination' parameters as per your specific requirements.
    model = IsolationForest(n_estimators=100, max_samples='auto', contamination=0.1, max_features=1.0)

    # Fit the model to the training data.
    model.fit(features_normalized)
    anomaly_scores = model.decision_function(features_normalized)
    threshold = -0.5 # This threshold value can be adjusted as per your specific requirements.
    risky_wallets = features[anomaly_scores < threshold]

    labels = model.predict(features_normalized)
    print(labels)
    # Check the anomaly labels.
    for i, label in enumerate(labels):
        if label == -1:
            print(f"Wallet {raw_df['address'][i]} is risky.")
            await bot.send_message(chat_id=channel_id, text=f"Wallet {raw_df['address'][i]} is risky.")

        else:
            print(f"Wallet {raw_df['address'][i]} is normal.")
            
            
if __name__ == '__main__':
    es = Elasticsearch(
        "http://34.143.255.36:9200/",basic_auth=("elastic","elastic2023"))
    bot = Bot(token='6845513846:AAHbmWW4Hhc35lnpIwTKW63o5RiZzA3-wSQ')
    asyncio.run( handleML(es,bot))
from analysis_functions import get_tickers_present, analyze_sentiment
from firebase_db import db
from flask import jsonify
from pubsub_client import publisher
import time


# triggered by a create (not update) to documents/raw_data/{data_id}
def analyze_raw_data(data, context):
    start_time = time.time()

    # first we isolate the path to the document so we can update the document
    path_parts = context.resource.split('/documents/')[1].split('/')
    collection_path = path_parts[0]
    document_path = '/'.join(path_parts[1:])
    raw_data_id = path_parts[-1]
    raw_data_doc = db().collection(collection_path).document(document_path)

    # here we set up the embedding pubsub publisher
    # this will allow us to push raw data that needs to be batch embedded to the correct topic
    embedding_publisher = publisher()
    project_id = "sentiment-investor-test"
    topic_id = "embed_text"
    embedding_topic_path = embedding_publisher.topic_path(project_id, topic_id)

    content = data["value"]["fields"]["content"]["stringValue"]
    datatype = data["value"]["fields"]["type"]["stringValue"]
    has_sentiment_score = data["value"]["fields"].get("sentiment", {}).get("doubleValue") is not None

    # set tickers to an empty list so that the variable is initialized
    tickers = []
    updated_fields = {}
    # yahoo finance comments are associated with their parent discussion's ticker
    # stocktwits posts are already labeled with tickers upon scraping
    # tweets are labeled with the tickers that they were queried with
    if datatype != "yahoo_finance_comment" and datatype != "stocktwits_post" and datatype != "tweet":
        tickers = get_tickers_present(content)
        updated_fields["tickers"] = tickers

    # if (len(tickers) > 0 and not (datatype == "stocktwits_post" and has_sentiment_score)) or datatype == "yahoo_finance_comment" or datatype == "tweet":
    if not ((datatype == "stocktwits_post" and has_sentiment_score) or (datatype == "reddit_comment" and len(tickers) == 0)):
        embedding_publisher.publish(embedding_topic_path, content.encode("utf-8"), raw_data_id=raw_data_id)

    updated_fields["level_0_analysis_timestamp"] = int(time.time())

    # first check level 0 analysis hasn't been performed
    # this prevents infinite write loops
    if "level_0_analysis_timestamp" not in data["value"]["fields"]:
        raw_data_doc.set(updated_fields, merge=True)

    return jsonify({
        "success": True,
        "time_taken": time.time() - start_time
    })

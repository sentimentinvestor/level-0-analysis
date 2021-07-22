from analysis_functions import get_tickers_present, analyze_sentiment
from firebase_db import db
from flask import jsonify
import time


# triggered by a write (not update) to documents/raw_data/{data_id}
def analyze_raw_data(data, context):
    start_time = time.time()

    # first we isolate the path to the document so we can update the document
    path_parts = context.resource.split('/documents/')[1].split('/')
    collection_path = path_parts[0]
    document_path = '/'.join(path_parts[1:])
    raw_data_doc = db().collection(collection_path).document(document_path)

    content = data["value"]["fields"]["content"]["stringValue"]
    type = data["value"]["fields"]["type"]["stringValue"]
    has_sentiment_score = data["value"]["fields"].get("sentiment") is not None

    # set tickers to an empty list so that the variable is initialized
    tickers = []
    updated_fields = {}
    # yahoo finance comments are associated with their parent discussion's ticker
    # stocktwits posts are already labeled with tickers upon scraping
    if type != "yahoo_finance_comment" and type != "stocktwits_post":
        tickers = get_tickers_present(content)
        updated_fields["tickers"] = tickers


    if len(tickers) > 0 and not (type == "yahoo_finance_comment" and has_sentiment_score):
        sentiment = analyze_sentiment(content)
        updated_fields["sentiment"] = sentiment

    updated_fields["level_0_analysis_timestamp"] = time.time()

    # first check level 0 analysis hasn't been performed
    # this prevents infinite write loops
    if not data["value"]["fields"].get("level_0_analysis_timestamp"):
        raw_data_doc.set(updated_fields, merge=True)

    return jsonify({
        "success": True,
        "time_taken": time.time() - start_time
    })
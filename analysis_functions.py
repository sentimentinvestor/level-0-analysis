import re
import json
import requests
from firebase_db import db

not_tickers = db().collection('targets') \
                            .document('common_tickers') \
                            .get() \
                            .to_dict()["not_tickers"]
reduced_ticker_list = db().collection('targets') \
                                    .document('reduced_tickers_list') \
                                    .get() \
                                    .to_dict()["reduced_tickers_list"]
with open('hidden/hidden_endpoints.json') as file:
    endpoints = json.load(file)


def get_tickers_present(text):
    ticker_pattern = r"(?:(?:\$([A-Za-z]{2,5})[\.,\s])|(?:[a-z0-9]+\s+([A-Z]{2,5})[\.,\s])|(?:[\.,\s]([A-Z]{2,5})\s+[a-z0-9]+))"
    ticker_regex = re.compile(ticker_pattern)
    matches = re.findall(ticker_regex, text)

    # remove this line if using the '$' only regex
    matches = [max(match).upper() for match in matches if max(match) != '']

    tickers_present = [m for m in matches if check_if_ticker(m)]

    return tickers_present


def check_if_ticker(ticker):
    if ticker in not_tickers:
        return False
    elif ticker in reduced_ticker_list:
        return True
    else:
        return False


def analyze_sentiment(text):
    endpoint = endpoints["nlp"]
    response = requests.get(endpoint, params={"text": text})
    return response.json()["sentiment"]
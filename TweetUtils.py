import requests
import json
import csv
from os import path
from dateutil import parser  # requires python-dateutil v 2.7.3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # requires vaderSentiment v3.2.1
from stanfordcorenlp import StanfordCoreNLP  # requires stanfordcorenlp v3.9.1.1
import logging


class Twutils:

    def format_tweet(self, data):
        # print(json.dumps(data, indent=4))
        tweet = [data["id_str"],
                 str(Twutils.get_date(data["created_at"])),
                 data["user"]["id_str"],
                 self.get_text(data),
                 self.get_sentiment(Twutils.get_text(data)),
                 data["retweet_count"],
                 data["favorite_count"]]
        return tweet

    @staticmethod
    def format_urls(id_str, urls):
        tweet_urls = []
        if len(urls) > 0:
            for url in urls:
                twurl = [url, id_str]
                tweet_urls.append(twurl)
        return tweet_urls

    @staticmethod
    def is_retweet(data):
        is_rt = False
        if "retweeted_status" in data:
            is_rt = True
        else:
            text = Twutils.get_text(data)
            if "RT @" in text:
                is_rt = True
        return is_rt

    @staticmethod
    def get_text(data):
        text = ""
        if "full_text" in data:  # search extended_mode=true
            text = data["full_text"]
        elif "extended_tweet" in data:  # streaming truncated tweet
            text = data["extended_tweet"]["full_text"]
        elif "text" in data:  # default
            text = data["text"]
        return text.replace('\n', '\\n')

    @staticmethod
    def get_date(date_str):
        parsed_date = parser.parse(date_str)
        return parsed_date

    @staticmethod
    def get_valid_urls(data):
        ret = []
        if "entities" in data:
            urls = data["entities"]["urls"]
            if len(urls) > 0:
                for url in urls:
                    exp_url = url["expanded_url"]
                    orig_url = Twutils.unshorten_url(exp_url)
                    if not orig_url == "" and not orig_url.startswith("https://twitter.com"):
                        ret.append([exp_url, orig_url])
        return ret

    @staticmethod
    def unshorten_url(url):
        try:
            r = requests.head(url, allow_redirects=True, timeout=10)
            return r.url
        except requests.exceptions.Timeout:
            return ""
        except requests.exceptions.TooManyRedirects:
            return ""
        except requests.exceptions.ConnectionError:
            return ""

    def get_sentiment(self, text):
        scores = self.sentiment_analyzer.polarity_scores(text)
        return scores["compound"]

    def get_entities(self, text):
        ners = set()
        sentences = json.loads(self.nlp.annotate(text, properties=self.nlp_props))
        # iterate over list of annotated sentences
        for sentence in sentences["sentences"]:
            # iterate over entity mentions for each sentence
            for entity in sentence["entitymentions"]:
                if entity["ner"] != "URL" and entity["ner"] != "EMAIL":
                    # add each named entity to a set
                    ners.add(entity["text"] + "|" + entity["ner"])
        return ners

    @staticmethod
    def format_ners(id_str, ners):
        lst = []
        for tuple in ners:
            entity = tuple.split('|')[0]
            lst.append([id_str, entity])
        return lst

    @staticmethod
    def append_to_csv_file(filepath, data, multi_rows=False):
        with open(filepath, newline='', mode='a+', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            if multi_rows:
                writer.writerows(data)
            else:
                writer.writerow(data)

    @staticmethod
    def add_to_dict(dict, urls):
        for url in urls:
            if url in dict:
                dict[url] += 1
            else:
                dict[url] = 1

    def __init__(self, ner=False, sentiment=False):
        if sentiment:
            self.sentiment_analyzer = SentimentIntensityAnalyzer()
        if ner:
            # stanford nlp pipeline properties
            self.nlp_props = {'annotators': 'tokenize, ssplit, truecase, pos, lemma, ner, entitymentions',
                              'outputFormat': 'json',
                              'truecase.overwriteText': 'true',
                              'pos.model': 'gate-EN-twitter.model',
                              'ner.model': 'edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz',
                              'ner.useSUTime': 'false',
                              'ner.applyNumericClassifiers': 'false'}
            # basepath: <root_dir>/TweetUtils.py
            basepath = path.dirname(path.abspath(__file__))
            # nlppath: <root_dir>/stanfordNLP/stanford-corenlp-full-2018-02-27
            nlppath = path.join(basepath, "stanfordNLP", "stanford-corenlp-full-2018-02-27")
            # nlp object with fixed path
            self.nlp = StanfordCoreNLP(nlppath, quiet=False, logging_level=logging.DEBUG)

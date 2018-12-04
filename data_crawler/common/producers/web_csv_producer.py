import urllib.request
import pandas as pd

from data_crawler.common.producers import Producer


class WebCSVProducer(Producer):

    NAME = NotImplementedError
    URL = NotImplementedError

    def get_full_content(self):
        return urllib.request.urlopen(self.URL)

    def parse_data(self, csv_content):
        return pd.read_csv(csv_content)
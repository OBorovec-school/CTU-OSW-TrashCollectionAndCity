import urllib.request
import json

from data_crawler.common.producers import Producer


class WebJsonProducer(Producer):

    NAME = NotImplementedError
    URL = NotImplementedError

    def get_full_content(self):
        with urllib.request.urlopen(self.URL) as url:
            data = json.loads(url.read().decode())
            return data

    def parse_data(self, json_object):
        raise NotImplementedError

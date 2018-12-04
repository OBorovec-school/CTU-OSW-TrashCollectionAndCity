import urllib

from data_crawler.common.producers import Producer


class WebPageProducer(Producer):

    NAME = NotImplementedError
    WEB_URL = NotImplementedError

    def get_full_content(self):
        fp = urllib.request.urlopen(self.WEB_URL)
        content = fp.read().decode("utf8")
        fp.close()
        return content

    def parse_data(self, html_content):
        raise NotImplementedError

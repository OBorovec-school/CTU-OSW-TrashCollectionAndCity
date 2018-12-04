import logging


class Producer:

    def __init__(self):
        self.logger = logging.getLogger('dc')

    def get_full_content(self):
        raise NotImplementedError

    def parse_data(self, data):
        raise NotImplementedError

    def get_entries(self):
        data = self.get_full_content()
        entries = self.parse_data(data)
        return entries

import logging


class Crawler:

    def __init__(self):
        self.logger = logging.getLogger('dc')

    def run(self):
        raise NotImplementedError
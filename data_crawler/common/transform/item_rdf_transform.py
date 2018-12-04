import logging
import pandas as pd
import rdflib
from rdflib import Namespace

from data_crawler.common.config import DCConfig


class ItemToRDFTransform:
    NAME = NotImplementedError
    NAMESPACE = NotImplementedError
    NAMESPACE_PREFIX = NotImplementedError

    def __init__(self):
        self.logger = logging.getLogger('dc')
        self.unparseble_logger = logging.getLogger('unparseble')
        self.rdf_output = DCConfig.get_rdf_type()

    def run(self, items):
        g = rdflib.Graph()
        n = Namespace(self.NAMESPACE)
        g.bind(self.NAMESPACE_PREFIX, n)
        if isinstance(items, list):
            for item in items:
                try:
                    self.parse_item_to_graph(item, g, n)
                except Exception as e:
                    self.logger.warning('Could not parse item with content: ' + str(e))
                    self.unparseble_logger.error(str(item))
        elif isinstance(items, pd.DataFrame):
            for index, item in items.iterrows():
                try:
                    self.parse_item_to_graph(item, g, n)
                except Exception as e:
                    self.logger.warning('Could not parse item with content: ' + str(e))
                    self.unparseble_logger.error(str(item))

        return g, n

    def parse_item_to_graph(self, item, g, n):
        raise NotImplementedError

import logging
import os
import rdflib

from data_crawler.common.config import DCConfig
from data_crawler.common.structure import get_result_file


class RDFFile:
    NAME = NotImplementedError
    OUTPUT_FILE = NotImplementedError

    def __init__(self):
        self.logger = logging.getLogger('dc')
        self.rdf_output = DCConfig.get_rdf_type()
        self.output_file = get_result_file(self.OUTPUT_FILE)

    def store(self, g, delete_firts=True):
        g_orig = rdflib.Graph()
        if os.path.isfile(self.output_file) and delete_firts:
            os.remove(self.output_file)
        elif os.path.isfile(self.output_file):
            g_orig.parse(self.output_file, format=self.rdf_output)
            g.parse(g_orig)
        g.serialize(destination=self.output_file, format=self.rdf_output)


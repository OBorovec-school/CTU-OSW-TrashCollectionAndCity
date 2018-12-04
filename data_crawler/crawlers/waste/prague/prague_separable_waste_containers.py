import hashlib
from collections import defaultdict

import rdflib
from rdflib import XSD, Literal

from data_crawler.common.producers.web_json_producer import WebJsonProducer
from data_crawler.common.sink.rdf_file_sink import RDFFile
from data_crawler.common.transform.item_rdf_transform import ItemToRDFTransform
from data_crawler.crawlers import Crawler

DATA_ENDPOINT = 'http://opendata.iprpraha.cz/CUR/ZPK/ZPK_O_Kont_TOitem_b/WGS_84/ZPK_O_Kont_TOitem_b.json'


class PragueSepWasteCrawler(Crawler):

    def run(self):
        self.logger.debug("Running :p")
        stations = PragueSepWasteProducer().get_entries()
        g, n = PragueSepWasteToRDF().run(stations)
        PragueSepWasteSink().store(g)

        self.logger.debug("DONE :p")


class PragueSepWasteProducer(WebJsonProducer):
    NAME = 'PragueSepWasteProducer'
    URL = DATA_ENDPOINT

    def parse_data(self, json_object):
        stations = defaultdict(list)
        features = json_object['features']
        for feature in features:
            loc = tuple(feature['geometry']['coordinates'])
            trash_type = feature['properties']['TRASHTYPENAME']
            cleaning_freq = feature['properties']['CLEANINGFREQUENCYCODE']
            container_type = feature['properties']['CONTAINERTYPE']
            stations[loc].append((trash_type, cleaning_freq, container_type))
        return list(stations.items())


class PragueSepWasteToRDF(ItemToRDFTransform):
    NAME = 'PragueSepWasteToRDF'
    NAMESPACE = 'PragueSepWaste/'
    NAMESPACE_PREFIX = 'psw'

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        lat = float(item[0][0])
        lon = float(item[0][1])
        containers = item[1]

        record = n[id]

        position = rdflib.BNode()
        g.add((record, n.position, position))
        g.add((position, n.lat, Literal(lat, datatype=XSD.float)))
        g.add((position, n.lon, Literal(lon, datatype=XSD.float)))

        containers_bag = rdflib.BNode()
        g.add((record, n.containers, containers_bag))

        for container in containers:
            container_info_bag = rdflib.BNode()
            g.add((containers_bag, n.container, container_info_bag))

            trash_type = container[0]
            cleaning_freq = int(container[1])
            container_type = container[2]
            g.add((container_info_bag, n.trash_type, Literal(trash_type, datatype=XSD.string)))
            g.add((container_info_bag, n.cleaning_freq, Literal(cleaning_freq, datatype=XSD.integer)))
            g.add((container_info_bag, n.container_type, Literal(container_type, datatype=XSD.string)))


class PragueSepWasteSink(RDFFile):
    NAME = 'PragueSepWasteSink'
    OUTPUT_FILE = 'PragueSepWaste.rdf'

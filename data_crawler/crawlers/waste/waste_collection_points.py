import hashlib
import json
from bs4 import BeautifulSoup
from rdflib import XSD, Literal

from data_crawler.common.producers.web_page_producer import WebPageProducer
from data_crawler.common.sink.rdf_file_sink import RDFFile
from data_crawler.common.transform.item_rdf_transform import ItemToRDFTransform
from data_crawler.crawlers import Crawler

DATA_ENDPOINT = 'https://www.sberne-dvory.cz/vyhledavani/?search_by=region&show=list&page_listing=30&city=56601&street=&zip=&distance=15&limit=&region%5B%5D=CZ.jic&region%5B%5D=CZ.jmo&region%5B%5D=CZ.kav&region%5B%5D=CZ.krh&region%5B%5D=CZ.lib&region%5B%5D=CZ.mor&region%5B%5D=CZ.olo&region%5B%5D=CZ.par&region%5B%5D=CZ.plz&region%5B%5D=CZ.pra&region%5B%5D=CZ.stc&region%5B%5D=CZ.ust&region%5B%5D=CZ.vys&region%5B%5D=CZ.zli'


class WasteCollectionPointsCrawler(Crawler):

    def run(self):
        self.logger.debug("Running :p")
        stations = WasteCollectionPointsProducer().get_entries()
        g, n = WasteCollectionPointsToRDF().run(stations)
        WasteCollectionPointsSink().store(g)

        self.logger.debug("DONE :p")


class WasteCollectionPointsProducer(WebPageProducer):
    NAME = 'WasteCollectionPointsProducer'
    WEB_URL = DATA_ENDPOINT

    def parse_data(self, html_page):
        elements = []
        for script_str in [ x.get_text() for x in BeautifulSoup(html_page, 'html.parser').find_all('script') if 'data.push' in x.get_text()]:
            json_str = script_str.split('data.push(')[-1]
            json_str = json_str.split(');')[0]
            try:
                elements.append(json.loads(json_str))
            except:
                pass
        return elements


class WasteCollectionPointsToRDF(ItemToRDFTransform):
    NAME = 'WasteCollectionPointsToRDF'
    NAMESPACE = 'WasteCollectionPoints/'
    NAMESPACE_PREFIX = 'wcp'

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        record = n[id]

        g.add((record, n.bin_types, Literal(item['Bin_Types'], datatype=XSD.string)))
        g.add((record, n.lat, Literal(float(item['Lat']), datatype=XSD.float)))
        g.add((record, n.lon, Literal(float(item['Long']), datatype=XSD.float)))
        g.add((record, n.name, Literal(item['Name'], datatype=XSD.string)))
        g.add((record, n.region, Literal(item['region'], datatype=XSD.string)))
        g.add((record, n.type, Literal(item['Type'], datatype=XSD.string)))
        g.add((record, n.open_Monday, Literal(item['Open_Monday'], datatype=XSD.string)))
        g.add((record, n.open_Tuesday, Literal(item['Open_Tuesday'], datatype=XSD.string)))
        g.add((record, n.open_Wednesday, Literal(item['Open_Wednesday'], datatype=XSD.string)))
        g.add((record, n.open_Thursday, Literal(item['Open_Thursday'], datatype=XSD.string)))
        g.add((record, n.opening_Friday, Literal(item['Open_Friday'], datatype=XSD.string)))
        g.add((record, n.open_Saturday, Literal(item['Open_Saturday'], datatype=XSD.string)))
        g.add((record, n.open_Sunday, Literal(item['Open_Sunday'], datatype=XSD.string)))


class WasteCollectionPointsSink(RDFFile):
    NAME = 'WasteCollectionPointsSink'
    OUTPUT_FILE = 'WasteCollectionPoints.rdf'

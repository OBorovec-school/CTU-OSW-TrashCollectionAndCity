import hashlib

import rdflib
from rdflib import Literal, XSD

from data_crawler.common.producers.web_csv_producer import WebCSVProducer
from data_crawler.common.sink.rdf_file_sink import RDFFile
from data_crawler.common.transform.item_rdf_transform import ItemToRDFTransform
from data_crawler.crawlers import Crawler

DATA_ENDPOINT = 'http://opendata.praha.eu/dataset/048f4737-0348-41e0-9f1f-7ca1adfa5e9b/resource/be9bc291-645e-4c8d-a0d1-ba206a05f033/download/77092d58-b39b-4159-a315-ddd4219cf300-4.csv'


class PragueWasteCollectionPointsCrawler(Crawler):

    def run(self):
        self.logger.debug("Running :p")
        points = PragueWasteCollectionPointsProducer().get_entries()
        g, n = PragueWasteCollectionPointsToRDF().run(points)
        PragueWasteCollectionPointsSink().store(g)

        self.logger.debug("DONE :p")


class PragueWasteCollectionPointsProducer(WebCSVProducer):
    NAME = 'PragueWasteCollectionPointsProducer'
    URL = DATA_ENDPOINT


class PragueWasteCollectionPointsToRDF(ItemToRDFTransform):
    NAME = 'PragueWasteCollectionPointsToRDF'
    NAMESPACE = 'PragueWasteCollectionPoints/'
    NAMESPACE_PREFIX = 'pwcp'

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        owner_id = item['IČ zřizovatle/majitele']
        owner_name = item['název zřizovatele/majitele']
        url = item['url']
        type = item['typ sběrného místa']
        openig_hours = item['provozní doba']
        accepted_waste = str(item['přijímaný odpad']).split(', ')
        contact_tel = item['kontakt (mobil, telefon)']
        contact_email = item['e-mail']
        location_address = item['adresa']
        location_lat = item['GPS umístění (zěmepisná šířka)']
        location_lon = item['GPS umístění (zěmepisná délka)']

        record = n[id]
        ownership = rdflib.BNode()
        g.add((record, n.ownership, ownership))
        g.add((ownership, n.owner_id, Literal(owner_id, datatype=XSD.string)))
        g.add((ownership, n.owner_name, Literal(owner_name, datatype=XSD.string)))
        g.add((record, n.url, Literal(url, datatype=XSD.anyURI)))
        g.add((record, n.type, Literal(type, datatype=XSD.string)))
        g.add((record, n.openig_hours, Literal(openig_hours, datatype=XSD.string)))
        accepted_waste_bag = rdflib.BNode()
        g.add((record, n.accepted_waste, accepted_waste_bag))
        for waste_type in accepted_waste:
            g.add((accepted_waste_bag, n.waste_type, Literal(waste_type, datatype=XSD.string)))
        contact = rdflib.BNode()
        g.add((record, n.contact, contact))
        g.add((contact, n.tel, Literal(contact_tel, datatype=XSD.string)))
        g.add((contact, n.emal, Literal(contact_email, datatype=XSD.string)))
        position = rdflib.BNode()
        g.add((record, n.position, position))
        g.add((position, n.address, Literal(location_address, datatype=XSD.string)))
        g.add((position, n.lat, Literal(location_lat, datatype=XSD.string)))
        g.add((position, n.lon, Literal(location_lon, datatype=XSD.string)))


class PragueWasteCollectionPointsSink(RDFFile):
    NAME = 'PragueWasteCollectionPointsSink'
    OUTPUT_FILE = 'PragueWasteCollectionPoints.rdf'

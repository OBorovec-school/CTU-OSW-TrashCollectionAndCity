import hashlib
import re
from bs4 import BeautifulSoup
import rdflib
from rdflib import XSD, Literal

from data_crawler.common.producers.web_json_producer import WebJsonProducer
from data_crawler.common.sink.rdf_file_sink import RDFFile
from data_crawler.common.transform.item_rdf_transform import ItemToRDFTransform
from data_crawler.crawlers import Crawler

DATA_ENDPOINT = 'https://www.sako.cz/pointerBig.php?tt=0.3011284183441809&kk=0.5409761154126591&daata=sberne_stredisko%3D1%26papir%3D1%26sklo_bile%3D1%26pet%3D1%26mestska_cast%3D%26ulice%3D%26ochrana%3D2'


class BrnoSepWasteCrawler(Crawler):

    def run(self):
        self.logger.debug("Running :p")
        stations = BrnoSepWasteProducer().get_entries()
        g, n = BrnoSepWasteToRDF().run(stations)
        BrnoSepWasteSink().store(g)

        self.logger.debug("DONE :p")


class BrnoSepWasteProducer(WebJsonProducer):
    NAME = 'BrnoSepWasteProducer'
    URL = DATA_ENDPOINT

    def parse_data(self, json_objects):
        elements = []
        for json_object in json_objects:
            values = {}
            name = str(json_object['nazev'])
            if ' - ' in name:
                name = name.split(' - ')
                values['address'] = name[0]
                values['placement'] = name[-1]
            else:
                values['address'] = name
                values['placement'] = 'None'
            values['lat'] = json_object['lat']
            values['lon'] = json_object['lng']
            html = BeautifulSoup(json_object['html'], 'html.parser')
            place_type:str = json_object['icon']

            if place_type.startswith('gfx/popelnice'):
                values['place_type'] = 'sso'
                for line in html.get_text('\n').split('\n'):
                    line = line.lstrip()
                    if line.startswith('Provozní doba: '):
                        values['opening_hours'] = line.split('Provozní doba: ')[-1]
                    if line.startswith('Telefon: '):
                        values['telefon'] = line.split('Telefon: ')[-1]
            elif place_type.startswith('gfx/pin'):
                values['place_type'] = 'containers'
                values['containers'] = []
                valid_headers = [x.get_text() for x in html.find_all('h3') if x.get_text() != '']
                parts = re.split('|'.join(valid_headers), html.get_text('\n'))
                for container_type, part in zip(valid_headers, parts[1:]):
                    container_values = {'type': container_type, 'volume': 'None', 'amount': 'None',
                                        'collection_day': 'None', 'collection_freq': 'None'}
                    for line in part.split('\n'):
                        line = line.lstrip()
                        if line.startswith('Objem: '):
                            container_values['volume'] = line.split('Objem: ')[-1]
                        if line.startswith('Počet: '):
                            container_values['amount'] = int(line.split('Počet: ')[-1])
                        if line.startswith('Svoz: '):
                            container_values['collection_day'] = line.split('Svoz: ')[-1]
                        if line.startswith('Četnost: '):
                            container_values['collection_freq'] = line.split('Četnost: ')[-1]
                    values['containers'].append(container_values)
            else:
                values['place_type'] = 'Unknown'
            elements.append(values)
        return elements


class BrnoSepWasteToRDF(ItemToRDFTransform):
    NAME = 'BrnoSepWasteToRDF'
    NAMESPACE = 'BrnoSepWaste/'
    NAMESPACE_PREFIX = 'bsw'

    def parse_item_to_graph(self, item, g, n):
        id = hashlib.md5(str(item).encode('utf-8')).hexdigest()

        record = n[id]

        position = rdflib.BNode()
        g.add((record, n.position, position))
        g.add((position, n.lat, Literal(item['lat'], datatype=XSD.float)))
        g.add((position, n.lon, Literal(item['lon'], datatype=XSD.float)))
        g.add((position, n.address, Literal(item['address'], datatype=XSD.string)))
        g.add((position, n.placement, Literal(item['placement'], datatype=XSD.string)))

        g.add((record, n.type, Literal(item['place_type'], datatype=XSD.string)))

        if item['place_type'] == 'sso':
            g.add((record, n.opening_hours, Literal(item['opening_hours'], datatype=XSD.string)))
            g.add((record, n.telefon, Literal(item['telefon'], datatype=XSD.string)))
        elif item['place_type'] == 'containers':
            containers_bag = rdflib.BNode()
            g.add((record, n.containers, containers_bag))

            for container in item['containers']:
                container_info_bag = rdflib.BNode()
                g.add((containers_bag, n.container, container_info_bag))

                g.add((container_info_bag, n.type, Literal(container['type'], datatype=XSD.string)))
                g.add((container_info_bag, n.volume, Literal(container['volume'], datatype=XSD.string)))
                g.add((container_info_bag, n.amount, Literal(container['amount'], datatype=XSD.integer)))
                g.add((container_info_bag, n.collection_day, Literal(container['collection_day'], datatype=XSD.string)))
                g.add((container_info_bag, n.collection_freq, Literal(container['collection_freq'], datatype=XSD.string)))


class BrnoSepWasteSink(RDFFile):
    NAME = 'BrnoSepWasteSink'
    OUTPUT_FILE = 'BrnoSepWaste.rdf'

import os
import re
import requests
import sqlalchemy as db

from .MardiEntities import MardiItem, MardiProperty
from .mardi_config import config
from .mathml_datatype import MathML
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_login import LoginError
from wikibaseintegrator.datatypes import (URL, CommonsMedia, ExternalID, Form, GeoShape, GlobeCoordinate, Item, Lexeme, Math, MonolingualText, MusicalNotation, Property, Quantity,
                                          Sense, String, TabularData, Time)

class MardiClient(WikibaseIntegrator):
    def __init__(self, **kwargs) -> None:
        super().__init__(is_bot=True)
        self.login = self.config(**kwargs)
        self.importer_api = config['IMPORTER_API_URL']
        self.item = MardiItem(api=self)
        self.property = MardiProperty(api=self)

    @staticmethod
    def config(user, password, login_with_bot=False):
        """
        Sets up initial configuration for the integrator

        Returns:
            Clientlogin object
        """
        wbi_config["MEDIAWIKI_API_URL"] = config['MEDIAWIKI_API_URL']
        wbi_config["SPARQL_ENDPOINT_URL"] = config['SPARQL_ENDPOINT_URL']
        wbi_config["WIKIBASE_URL"] = config['WIKIBASE_URL']
        try:
            if login_with_bot:
                return wbi_login.Login(
                    user=user,
                    password=password
                )
            else:
                return wbi_login.Clientlogin(
                    user=user,
                    password=password
                )
        except LoginError:
            print('Wrong credentials')

    def get_local_id_by_label(self, entity_str, entity_type):
        """Check if entity with a given label or wikidata PID/QID 
        exists in the local wikibase instance. 

        Args:
            entity_str (str): It can be a string label or a wikidata ID, 
               specified with the prefix wdt: for properties and wd:
                for items.
            entity_type (str): Either 'property' or 'item' to specify
                which type of entity to look for.

        Returns:
           str: Local ID of the entity, if found.
        """
        local_pattern = r'^[PQ]\d+$'
        wikidata_pattern = r'^wdt?:([PQ]\d+$)'
        if re.match(local_pattern, entity_str):
            return entity_str
        elif not entity_str.startswith("wdt:") and not entity_str.startswith("wd:"):
            if entity_type == "property":
                new_property = MardiProperty(api=self).new()
                new_property.labels.set(language='en', value=entity_str)
                return new_property.get_PID()
            elif entity_type == "item":
                new_item = MardiItem(api=self).new()
                new_item.labels.set(language='en', value=entity_str)
                return new_item.get_QID()
        elif re.match(wikidata_pattern, entity_str):
            match = re.match(wikidata_pattern, entity_str)
            wikidata_id = match.group(1)
            if wikidata_id.startswith("Q"):
                response = requests.get(f'{self.importer_api}/items/{entity_str}/mapping')
                response = response.json()
                return response.get('local_id')
            elif wikidata_id.startswith("P"):
                response = requests.get(f'{self.importer_api}/properties/{entity_str}/mapping')
                response = response.json()
                return response.get('local_id')     

    def search_entity_by_value(self, prop_nr, value):
        prefix = None
        if (wbi_config['SPARQL_ENDPOINT_URL'] == 
            'http://query.portal.mardi4nfdi.de/proxy/wdqs/bigdata/namespace/wdq/sparql'):
            prefix = "PREFIX wdt: <https://portal.mardi4nfdi.de/prop/direct/>"

        prop_nr = self.get_local_id_by_label(prop_nr, 'property')
        if isinstance(value, str): 
            value = f'"{value}"'

        query = f'SELECT ?item WHERE {{?item wdt:{prop_nr} {value}}}'
        result = execute_sparql_query(query, prefix)

        QID_list = []
        for item in result['results']['bindings']:
            match = re.search(r'\/(Q\d+)$', item['item']['value'])
            QID = match.group(1)
            QID_list.append(QID)
        return QID_list    

    def get_claim(self, prop_nr, value=None, **kwargs):
        """
        Creates the appropriate claim to be inserted, which 
        correponds to the given property

        Args:
            prop_nr (str): Property correspoding to the claim. It
                can be a wikidata ID with the prefix 'wdt:', a
                mardi ID, or directly the property label.
            value (str): Value corresponding to the claim. In case
                of an item, the wikidata ID can be used with the
                prefix 'wd:'.

        Returns:
            Claim: Claim corresponding to the given datatype

        """
        prop_nr = self.get_local_id_by_label(prop_nr, 'property')
        try:
            prop = self.property.get(entity_id=prop_nr)
        except ValueError:
            datatype = "mathml"
        else:
            datatype = prop.datatype.value
        kwargs['prop_nr'] = prop_nr
        kwargs['value'] = value
        if datatype == 'wikibase-item':
            if value.startswith("wd:"):
                kwargs['value'] = self.get_local_id_by_label(value, 'item')
            elif value in ['MaRDI person profile',
                           'MaRDI publication profile',
                           'MaRDI software profile',
                           'MaRDI formula profile',
                           'MaRDI dataset profile',
                           'MaRDI community profile']:
                kwargs['value'] = self.get_local_id_by_label(value, 'item')
            return Item(**kwargs)
        elif datatype == 'commonsMedia':
            return CommonsMedia(**kwargs)
        elif datatype == 'external-id':
            return ExternalID(**kwargs)
        elif datatype == 'wikibase-form':
            return Form(**kwargs)
        elif datatype == 'geo-shape':
            return GeoShape(**kwargs)
        elif datatype == 'globe-coordinate':
            return GlobeCoordinate(**kwargs)
        elif datatype == 'wikibase-lexeme':
            return Lexeme(**kwargs)
        elif datatype == 'math':
            return Math(**kwargs)
        elif datatype == 'monolingualtext':
            kwargs['text'] = value
            kwargs.pop("value")
            return MonolingualText(**kwargs)
        elif datatype == 'musical-notation':
            return MusicalNotation(**kwargs)
        elif datatype == 'wikibase-property':
            return Property(**kwargs)
        elif datatype == 'quantity':
            kwargs['amount'] = value
            kwargs.pop("value")
            return Quantity(**kwargs)
        elif datatype == 'wikibase-sense':
            return Sense(**kwargs)
        elif datatype == 'string':
            return String(**kwargs)
        elif datatype == 'tabular-data':
            return TabularData(**kwargs)
        elif datatype == 'time':
            kwargs['time'] = value
            kwargs.pop("value")
            return Time(**kwargs)
        elif datatype == 'url':
            return URL(**kwargs)
        elif datatype == 'mathml':
            return MathML(**kwargs)

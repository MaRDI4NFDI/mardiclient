import re
import os
import requests
import sqlalchemy as db
from sqlalchemy import and_

from wikibaseintegrator.entities import ItemEntity, PropertyEntity
from wikibaseintegrator.wbi_exceptions import ModificationFailed
from wikibaseintegrator.datatypes import ExternalID
from wikibaseintegrator.wbi_enums import ActionIfExists

class MardiItem(ItemEntity):

    def new(self, **kwargs):
        return MardiItem(api=self.api, **kwargs)
    
    def write(self, **kwargs):
        try:
            entity = super().write(**kwargs)
            return entity
        except ModificationFailed as e:
            return self.handleModificationFailed(e)

    def handleModificationFailed(self, e):
        """Handle ModificationFailed Exception
        """
        match = re.search(r'Q\d+', str(e))
        if match:
            qid = match.group()
            print('Item with given label and description already exists.')
            print(f'Existing item with QID: {qid} is returned')
            return self.api.item.get(entity_id=qid)
        else:
            print(e)

    def get(self, entity_id, **kwargs):
        json_data = super(ItemEntity, self)._get(entity_id=entity_id, **kwargs)
        return MardiItem(api=self.api).from_json(json_data=json_data['entities'][entity_id])

    def exists(self): 
        """Checks through the Wikibase DB if an item with same label
        and description already exists

        Returns:
            id (str): ID of the item if found, otherwise None.
        """

        description = ""
        if 'en' in self.descriptions.values:
            description = self.descriptions.values['en'].value

        # List of items with the same label
        QID_list = self.get_QID()

        # Check if there is an item with the same description
        for QID in QID_list:
            item = ItemEntity(api=self.api).new()
            item = item.get(QID)
            if description == item.descriptions.values.get('en'):
                return QID

    def add_claim(self, prop_nr, value=None, action="append_or_replace", **kwargs):
        """
        Add a single claim to the item, given the property and
        it value. Qualifiers and references can also be passed.

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
        claim = self.api.get_claim(prop_nr, value, **kwargs)
        if action == "append_or_replace":
            action = ActionIfExists.APPEND_OR_REPLACE
        elif action == "replace_all":
            action = ActionIfExists.REPLACE_ALL
        else:
            action = ActionIfExists.APPEND_OR_REPLACE
        self.claims.add(claim, action)

    def is_instance_of(self, instance):
        """Checks if a given entity is an instance of 'instance' item

        (e.g. Check if an item is an instance of 'scholary article')

        Args:
            instance (str): Identifier for instance. The prefix "wd:" 
                can be used for items for wikidata.

        Returns:
            id (str): ID of the item if found, otherwise None.
        """
        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        instance_QID = self.api.get_local_id_by_label(instance, 'item')
        if type(instance_QID) is list: instance_QID = instance_QID[0]

        instance_of_PID = self.api.get_local_id_by_label('instance of', 'property')

        item_QID_list = self.get_QID()
        for QID in item_QID_list:
            item = self.api.item.get(QID)
            item_claims = item.get_json()['claims']
            if instance_of_PID in item_claims:
                if (instance_QID == 
                    item_claims[instance_of_PID][0]['mainsnak']['datavalue']['value']['id']):
                    return item.id
        return False

    def get_instance_list(self, instance):
        """Returns all items that have the same label, or an alias with
        the label, and are an instance of 'instance'
        """
        instance_QID = self.api.get_local_id_by_label(instance, 'item')
        if type(instance_QID) is list: instance_QID = instance_QID[0]

        instance_of_PID = self.api.get_local_id_by_label('instance of', 'property')

        item_QID_list = self.get_QID(alias=True)
        items = []
        for QID in item_QID_list:
            item = self.api.item.get(QID)
            item_claims = item.get_json()['claims']
            if instance_of_PID in item_claims:
                if (instance_QID == 
                    item_claims[instance_of_PID][0]['mainsnak']['datavalue']['value']['id']):
                    items.append(item.id)
        return items

    def is_instance_of_with_property(self, instance, prop_str, value):
        """Checks if a given entity is an instance of 'instance' item 
        an has a property equal to the given value.

        (e.g. Check if an item is an instance of 'scholary article' and
        has a DOI equal to 'value')

        Args:
            instance (str): Identifier for instance. The prefix "wd:" 
                can be used for items for wikidata.
            prop_nr (str): Property to be checked. It can be a 
                wikidata ID with the prefix 'wdt:', a mardi ID, or 
                directly the property label.
            value (str): Value corresponding to the property. In case
                of an item, the wikidata ID can be used with the
                prefix 'wd:'.

        Returns:
            id (str): ID of the item if found, otherwise None.
        """
        item_QID_list = self.get_instance_list(instance)
        prop_nr = self.api.get_local_id_by_label(prop_str, 'property')
        for item_QID in item_QID_list:
            item = self.api.item.get(item_QID)
            item_claims = item.get_json()['claims']
            values = self.__return_values(prop_nr, item_claims)
            if value in values: return item_QID

    def get_value(self, prop_str):
        """
        Returns all values of an item in the statement for a given
        property

        Args:
            prop_nr (str): Property to be checked. It can be a 
                wikidata ID with the prefix 'wdt:', a mardi ID, or 
                directly the property label.

        Returns:
            values (list): List of all values appearing in the 
                statement corresponding to the property.

        """
        QID = self.id if self.id else self.exists()
        if QID:
            item = ItemEntity(api=self.api).new()
            item = item.get(QID)
            item_claims = item.get_json()['claims']
            prop_nr = self.api.get_local_id_by_label(prop_str, 'property')
            return self.__return_values(prop_nr, item_claims)
    
    def __return_values(self, prop_nr, claims):
        """
        Internal method to process a set of claims and return the 
        values corresponding to prop_nr

        Args:
            prop_nr: ID corresponding to the property
            claims: Claims to be processed corresponding to an item.

        Returns:
            values (list): List of all values appearing in the 
                given claims corresponding to prop_nr.
        """
        values = []
        if prop_nr in claims:
            for mainsnak in claims[prop_nr]:
                if mainsnak['mainsnak']['datatype'] in ['string', 'external-id']:
                    values.append(mainsnak['mainsnak']['datavalue']['value'])
                elif mainsnak['mainsnak']['datatype'] == 'wikibase-item':
                    values.append(mainsnak['mainsnak']['datavalue']['value']['id'])
                elif mainsnak['mainsnak']['datatype'] == 'time':
                    values.append(mainsnak['mainsnak']['datavalue']['value']['time'])
        return values

    def get_QID(self):
        """Creates a list of QID of all items in the local wikibase with the
        same label

        Returns:
            QIDs (list): List of QID
        """
        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        importer_endpoint = self.api.importer_api
        response = requests.get(f'{importer_endpoint}/search/items/{label}')
        response = response.json()
        return response.get('QID') or []

    def add_linker_claim(self, wikidata_id):
        """Function for in-place addition of a claim with the
        property that points to the wikidata id
        to the local entity

        Args:
            entity: WikibaseIntegrator entity whose claims
                    this should be added to
            wikidata_id: wikidata id of the wikidata item
        """
        claim = ExternalID(
            value=wikidata_id,
            prop_nr=self.api.wikidata_QID,
        )
        self.add_claims(claim)
    

class MardiProperty(PropertyEntity):
    def new(self, **kwargs):
        return MardiProperty(api=self.api, **kwargs)

    def get(self, entity_id, **kwargs):
        json_data = super(PropertyEntity, self)._get(entity_id=entity_id, **kwargs)
        return MardiProperty(api=self.api).from_json(json_data=json_data['entities'][entity_id])

    def exists(self): 
        """Checks through the Wikibase DB if a property with the 
        same label already exists.
        """
        return self.get_PID()

    def get_PID(self):
        """Returns the PID of the property with the same label
        """

        label = ""
        if 'en' in self.labels.values:
            label = self.labels.values['en'].value

        importer_endpoint = self.api.importer_api
        response = requests.get(f'{importer_endpoint}/search/properties/{label}')
        response = response.json()
        return response.get('PID') or []

    def add_linker_claim(self, wikidata_id):
        """Function for in-place addition of a claim with the
        property that points to the wikidata id
        to the local entity

        Args:
            entity: WikibaseIntegrator entity whose claims
                    this should be added to
            wikidata_id: wikidata id of the wikidata item
        """
        claim = ExternalID(
            value=wikidata_id,
            prop_nr=self.api.wikidata_PID,
        )
        self.add_claims(claim)

    def add_claim(self, prop_nr, value=None, action="append_or_replace", **kwargs):
        """
        Add a single claim to the property, given the property and
        it value. Qualifiers and references can also be passed.

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
        claim = self.api.get_claim(prop_nr, value, **kwargs)
        if action == "append_or_replace":
            action = ActionIfExists.APPEND_OR_REPLACE
        elif action == "replace_all":
            action = ActionIfExists.REPLACE_ALL
        else:
            action = ActionIfExists.APPEND_OR_REPLACE
        self.claims.add(claim, action)

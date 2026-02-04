"""MaRDI entity classes for items and properties."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

import requests
from wikibaseintegrator.entities import ItemEntity, PropertyEntity
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_exceptions import ModificationFailed


if TYPE_CHECKING:
    from .mardi_client import MardiClient


class MardiItem(ItemEntity):
    """Extended ItemEntity with MaRDI-specific functionality."""

    api: MardiClient

    def new(self, **kwargs: Any) -> MardiItem:
        """Create a new MaRDI item instance.

        Returns:
            New MardiItem instance
        """
        return MardiItem(api=self.api, **kwargs)

    def write(self, **kwargs: Any) -> MardiItem:
        """Write the item to the knowledge graph.

        Returns:
            The written item

        Raises:
            ModificationFailed: If the write operation fails
        """
        try:
            entity = super().write(**kwargs)
            return cast("MardiItem", entity)
        except ModificationFailed as e:
            existing_item = self._handle_modification_failed(e)
            if existing_item:
                return existing_item
            raise

    def _handle_modification_failed(self, e: ModificationFailed) -> MardiItem | None:
        """Handle ModificationFailed exception by finding existing item.

        Args:
            e: The ModificationFailed exception

        Returns:
            Existing item if found, None otherwise
        """
        # Check for the specific duplicate label/description conflict
        if "wikibase-validator-label-with-description-conflict" not in e.messages_names:
            return None

        entity_link = e.messages[0]["parameters"][2]
        match = re.search(r"Q\d+", entity_link)
        if match:
            qid = match.group()
            print(f"Found existing item {qid}, returning it")
            return self.api.item.get(entity_id=qid)

    def get(self, entity_id: str, **kwargs: Any) -> MardiItem:
        """Get an item by its entity ID.

        Args:
            entity_id: The QID of the item
            **kwargs: Additional parameters

        Returns:
            MardiItem instance
        """
        json_data = super(ItemEntity, self)._get(entity_id=entity_id, **kwargs)
        item = MardiItem(api=self.api).from_json(json_data=json_data["entities"][entity_id])
        return cast("MardiItem", item)

    def exists(self) -> str | None:
        """Check if an item with the same label and description exists.

        Returns:
            QID of existing item if found, None otherwise
        """

        label = self.labels.values["en"].value if "en" in self.labels.values else ""
        description = (
            self.descriptions.values["en"].value if "en" in self.descriptions.values else ""
        )

        # List of items with the same label or alias
        QID_list = self.get_QID()

        # Check if there is an item with the same label and description
        for QID in QID_list:
            item = ItemEntity(api=self.api).get(QID)
            if description == item.descriptions.values.get(
                "en"
            ) and label == item.labels.values.get("en"):
                return QID

        return None

    def add_claim(
        self, prop_nr: str, value: Any = None, action: str = "append_or_replace", **kwargs: Any
    ) -> None:
        """Add a claim to the item.

        Args:
            prop_nr: Property identifier (wdt:PID, PID, or label)
            value: Value for the claim. The wikidata ID can be used with the prefix 'wd:'
            action: Action to take if claim exists ("append_or_replace" or "replace_all")
            **kwargs: Additional parameters for the claim (qualifiers, references)
        """
        claim = self.api.get_claim(prop_nr, value, **kwargs)

        action_map = {
            "append_or_replace": ActionIfExists.APPEND_OR_REPLACE,
            "replace_all": ActionIfExists.REPLACE_ALL,
        }

        action_enum = action_map.get(action, ActionIfExists.APPEND_OR_REPLACE)

        self.claims.add(claim, action_enum)

    def is_instance_of(self, instance: str) -> str | bool:
        """Check if the item is an instance of the specified type.

        Args:
            instance: Instance type identifier (wd:Q123, Q123, or label)

        Returns:
            QID if item is an instance of the type, False otherwise
        """
        instance_QID = self.api.get_local_id_by_label(instance, "item")
        if isinstance(instance_QID, list):
            instance_QID = instance_QID[0] if instance_QID else None

        if not instance_QID:
            return False

        instance_of_PID = self.api.get_local_id_by_label("instance of", "property")
        if not instance_of_PID:
            return False

        item_QID_list = self.get_QID()
        for QID in item_QID_list:
            item = self.api.item.get(QID)
            item_claims = item.get_json()["claims"]
            if instance_of_PID in item_claims and (
                instance_QID
                == item_claims[instance_of_PID][0]["mainsnak"]["datavalue"]["value"]["id"]
            ):
                return str(item.id)

        return False

    def get_instance_list(self, instance: str) -> list[str]:
        """Get all items with the same label that are instances of the specified type.

        Args:
            instance: Instance type identifier

        Returns:
            List of QIDs
        """
        instance_QID = self.api.get_local_id_by_label(instance, "item")
        if isinstance(instance_QID, list):
            instance_QID = instance_QID[0] if instance_QID else None

        if not instance_QID:
            return []

        instance_of_PID = self.api.get_local_id_by_label("instance of", "property")
        if not instance_of_PID:
            return []

        item_QID_list = self.get_QID()
        items = []

        for QID in item_QID_list:
            item = self.api.item.get(QID)
            item_claims = item.get_json()["claims"]
            if instance_of_PID in item_claims and (
                instance_QID
                == item_claims[instance_of_PID][0]["mainsnak"]["datavalue"]["value"]["id"]
            ):
                items.append(item.id)

        return items

    def is_instance_of_with_property(self, instance: str, prop_str: str, value: Any) -> str | None:
        """Check if item is an instance of specified type with a given property value.

        Args:
            instance: Instance type identifier. The prefix "wd:" can be used for
                wikidata items.
            prop_str: Property identifier (wd:P123, P123, or label)
            value: Expected property value. Wikidata IDscan be used with the
                prefix 'wd:'.

        Returns:
            QID if found, None otherwise
        """
        item_QID_list = self.get_instance_list(instance)
        prop_nr = self.api.get_local_id_by_label(prop_str, "property")

        if not prop_nr:
            return None

        if isinstance(prop_nr, list):
            prop_nr = prop_nr[0]

        for item_QID in item_QID_list:
            item = self.api.item.get(item_QID)
            item_claims = item.get_json()["claims"]
            values = self._extract_values(prop_nr, item_claims)

            if value in values:
                return item_QID

        return None

    def get_value(self, prop_str: str) -> list[Any]:
        """Get all values for a given property on this item.

        Args:
            prop_str: Property identifier (wdt:PID, PID, or label)

        Returns:
            List of values for the property
        """
        QID = self.id if self.id else self.exists()

        if QID:
            item = ItemEntity(api=self.api).new()
            item = item.get(QID)
            item_claims = item.get_json()["claims"]
            prop_nr = self.api.get_local_id_by_label(prop_str, "property")

            if not prop_nr:
                return []

            if isinstance(prop_nr, list):
                prop_nr = prop_nr[0]

            return self._extract_values(prop_nr, item_claims)

        return []

    def _extract_values(self, prop_nr: str, claims: dict[str, Any]) -> list[Any]:
        """Extract values from claims for a given property.

        Args:
            prop_nr: Property identifier
            claims: Claims to be processed corresponding to an item.

        Returns:
            List of extracted values
        """
        values: list[Any] = []

        if prop_nr not in claims:
            return values

        for mainsnak in claims[prop_nr]:
            datatype = mainsnak["mainsnak"]["datatype"]
            datavalue = mainsnak["mainsnak"].get("datavalue")

            if not datavalue:
                continue

            if datatype in ["string", "external-id"]:
                values.append(datavalue["value"])
            elif datatype == "wikibase-item":
                values.append(datavalue["value"]["id"])
            elif datatype == "time":
                values.append(datavalue["value"]["time"])

        return values

    def get_QID(self) -> list[str]:
        """Get list of QIDs for items with the same label.

        Returns:
            List of QIDs
        """
        label = self.labels.values["en"].value if "en" in self.labels.values else ""

        if not label:
            return []

        response = requests.get(
            f"{self.api.importer_api}/search/items/{label}",
            timeout=60,
        )
        response.raise_for_status()
        return response.json().get("QID") or []


class MardiProperty(PropertyEntity):
    """Extended PropertyEntity with MaRDI-specific functionality."""

    api: MardiClient

    def new(self, **kwargs: Any) -> MardiProperty:
        """Create a new MaRDI property instance.

        Returns:
            New MardiProperty instance
        """
        return MardiProperty(api=self.api, **kwargs)

    def get(self, entity_id: str, **kwargs: Any) -> MardiProperty:
        """Get a property by its entity ID.

        Args:
            entity_id: The PID of the property
            **kwargs: Additional parameters

        Returns:
            MardiProperty instance
        """
        json_data = super(PropertyEntity, self)._get(entity_id=entity_id, **kwargs)
        return cast(
            "MardiProperty",
            MardiProperty(api=self.api).from_json(json_data=json_data["entities"][entity_id]),
        )

    def exists(self) -> list[str]:
        """Check if a property with the same label exists.

        Returns:
            List of PIDs with matching labels
        """
        return self.get_PID()

    def get_PID(self) -> list[str]:
        """Get PIDs of properties with the same label.

        Returns:
            List of PIDs
        """
        label = self.labels.values["en"].value if "en" in self.labels.values else ""

        if not label:
            return []

        response = requests.get(
            f"{self.api.importer_api}/search/properties/{label}",
            timeout=60,
        )
        response.raise_for_status()
        return response.json().get("PID") or []

    def add_claim(
        self,
        prop_nr: str,
        value: Any = None,
        action: str = "append_or_replace",
        **kwargs: Any,
    ) -> None:
        """Add a claim to the property.

        Args:
            prop_nr: Property identifier (wdt:PID, PID, or label)
            value: Value for the claim
            action: Action to take if claim exists ("append_or_replace" or "replace_all")
            **kwargs: Additional parameters for the claim (qualifiers, references)
        """
        claim = self.api.get_claim(prop_nr, value, **kwargs)

        action_map = {
            "append_or_replace": ActionIfExists.APPEND_OR_REPLACE,
            "replace_all": ActionIfExists.REPLACE_ALL,
        }
        action_enum = action_map.get(action, ActionIfExists.APPEND_OR_REPLACE)

        self.claims.add(claim, action_enum)

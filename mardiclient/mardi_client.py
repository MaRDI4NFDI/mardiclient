"""MaRDI Client for interacting with the MaRDI knowledge graph."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import requests
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.datatypes import (
    URL,
    CommonsMedia,
    ExternalID,
    Form,
    GeoShape,
    GlobeCoordinate,
    Item,
    Lexeme,
    Math,
    MonolingualText,
    MusicalNotation,
    Property,
    Quantity,
    Sense,
    String,
    TabularData,
    Time,
)
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query
from wikibaseintegrator.wbi_login import LoginError

from .mardi_config import config
from .mardi_entities import MardiItem, MardiProperty
from .mathml_datatype import MathML


if TYPE_CHECKING:
    from wikibaseintegrator.datatypes import BaseDataType


class MardiClient(WikibaseIntegrator):
    """Client for interacting with the MaRDI knowledge graph.

    This client extends WikibaseIntegrator to provide convenient methods
    for working with MaRDI items and properties.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the MaRDI client.

        Args:
            **kwargs: Configuration options including:
                - user: Username for authentication
                - password: Password for authentication
                - login_with_bot: Whether to use bot login (default: False)
                - mediawiki_api_url: Override default MediaWiki API URL
                - sparql_endpoint_url: Override default SPARQL endpoint
                - wikibase_url: Override default Wikibase URL
                - user_agent: Override default user agent
                - importer_api_url: Override default importer API URL
        """
        super().__init__(is_bot=True)

        mediawiki_api_url = kwargs.pop("mediawiki_api_url", config["MEDIAWIKI_API_URL"])
        sparql_endpoint_url = kwargs.pop("sparql_endpoint_url", config["SPARQL_ENDPOINT_URL"])
        wikibase_url = kwargs.pop("wikibase_url", config["WIKIBASE_URL"])
        user_agent = kwargs.pop("user_agent", config["USER_AGENT"])
        self.importer_api = kwargs.pop("importer_api_url", config["IMPORTER_API_URL"])

        self.login = self._config(
            mediawiki_api_url=mediawiki_api_url,
            sparql_endpoint_url=sparql_endpoint_url,
            wikibase_url=wikibase_url,
            user_agent=user_agent,
            **kwargs,
        )

        self.item = MardiItem(api=self)
        self.property = MardiProperty(api=self)

    @staticmethod
    def _config(
        mediawiki_api_url: str,
        sparql_endpoint_url: str,
        wikibase_url: str,
        user_agent: str,
        **kwargs: Any,
    ) -> wbi_login.Login | wbi_login.Clientlogin | None:
        """Configure the Wikibase integrator settings.

        Args:
            mediawiki_api_url: MediaWiki API endpoint URL
            sparql_endpoint_url: SPARQL endpoint URL
            wikibase_url: Wikibase instance URL
            user_agent: User agent string for requests
            **kwargs: Additional configuration including user, password, login_with_bot

        Returns:
            Login object if credentials provided, None otherwise
        """
        wbi_config["MEDIAWIKI_API_URL"] = mediawiki_api_url
        wbi_config["SPARQL_ENDPOINT_URL"] = sparql_endpoint_url
        wbi_config["WIKIBASE_URL"] = wikibase_url
        wbi_config["USER_AGENT"] = user_agent

        user = kwargs.get("user")
        password = kwargs.get("password")
        login_with_bot = kwargs.get("login_with_bot", False)

        if not user or not password:
            return None

        try:
            if login_with_bot:
                return wbi_login.Login(user=user, password=password)
            else:
                return wbi_login.Clientlogin(user=user, password=password)
        except LoginError:
            print("Wrong credentials")
            return None

    def get_local_id_by_label(self, entity_str: str, entity_type: str) -> str | list[str] | None:
        """Check if entity with a given label or wikidata PID/QID
        exists in the local wikibase instance.

        Args:
            entity_str: Either a label string, local ID (P123/Q123),
                       or Wikidata ID with prefix (wdt:P123/wd:Q123)
            entity_type: Either "property" or "item"

        Returns:
            Local ID(s) of the entity if found, None otherwise
        """
        local_pattern = r"^[PQ]\d+$"
        wikidata_pattern = r"^wdt?:([PQ]\d+$)"

        if re.match(local_pattern, entity_str):
            return entity_str

        if not entity_str.startswith(("wdt:", "wd:")):
            if entity_type == "property":
                new_property = MardiProperty(api=self).new()
                new_property.labels.set(language="en", value=entity_str)
                return new_property.get_PID()
            elif entity_type == "item":
                new_item = MardiItem(api=self).new()
                new_item.labels.set(language="en", value=entity_str)
                return new_item.get_QID()

        match = re.match(wikidata_pattern, entity_str)
        if match:
            wikidata_id = match.group(1)
            endpoint = "items" if wikidata_id.startswith("Q") else "properties"
            try:
                response = requests.get(
                    f"{self.importer_api}/{endpoint}/{entity_str}/mapping",
                    timeout=30,
                )
                response.raise_for_status()
                return str(response.json().get("local_id"))
            except requests.RequestException:
                return None

        return None

    def search_entity_by_value(self, prop_nr: str, value: str | int) -> list[str]:
        """Search for entities by property value.

        Args:
            prop_nr: Property identifier
            value: Value to search for

        Returns:
            List of QIDs matching the search criteria
        """
        resolved_prop = self.get_local_id_by_label(prop_nr, "property")
        if not resolved_prop or isinstance(resolved_prop, list):
            return []

        prop_nr = resolved_prop

        formatted_value = f'"{value}"' if isinstance(value, str) else value
        query = f"SELECT ?item WHERE {{?item wdt:{prop_nr} {formatted_value}}}"

        try:
            result = execute_sparql_query(query)
        except Exception:
            return []

        QID_list = []
        for item in result["results"]["bindings"]:
            match = re.search(r"\/(Q\d+)$", item["item"]["value"])
            if match:
                QID_list.append(match.group(1))

        return QID_list

    def get_claim(self, prop_nr: str, value: Any = None, **kwargs: Any) -> BaseDataType | MathML:
        """
        Creates the appropriate claim to be inserted, which
        correponds to the given property

        Args:
            prop_nr: Property identifier (can be wdt:PID, PID, or label)
            value: Value for the claim. In case of an item the wikidata
                ID can be used with the prefix 'wd:'.
            **kwargs: Additional parameters for the claim

        Returns:
            Appropriate claim object for the property's datatype
        """
        resolved_prop = self.get_local_id_by_label(prop_nr, "property")
        if not resolved_prop:
            return []
        if isinstance(resolved_prop, list):
            resolved_prop = resolved_prop[0]

        prop_nr = resolved_prop

        try:
            prop = self.property.get(entity_id=prop_nr)
            datatype = prop.datatype.value
        except ValueError:
            datatype = "mathml"

        kwargs["prop_nr"] = prop_nr
        kwargs["value"] = value
        if datatype == "wikibase-item":
            if value.startswith("wd:"):
                kwargs["value"] = self.get_local_id_by_label(value, "item")
            elif value in [
                "MaRDI person profile",
                "MaRDI publication profile",
                "MaRDI software profile",
                "MaRDI formula profile",
                "MaRDI dataset profile",
                "MaRDI community profile",
            ]:
                item_id = self.get_local_id_by_label(value, "item")
                if isinstance(item_id, list):
                    kwargs["value"] = item_id[0]
                elif item_id is None:
                    raise ValueError(f"Could not find item for: {value}")
                else:
                    kwargs["value"] = item_id
            return Item(**kwargs)
        elif datatype == "commonsMedia":
            return CommonsMedia(**kwargs)
        elif datatype == "external-id":
            return ExternalID(**kwargs)
        elif datatype == "wikibase-form":
            return Form(**kwargs)
        elif datatype == "geo-shape":
            return GeoShape(**kwargs)
        elif datatype == "globe-coordinate":
            return GlobeCoordinate(**kwargs)
        elif datatype == "wikibase-lexeme":
            return Lexeme(**kwargs)
        elif datatype == "math":
            return Math(**kwargs)
        elif datatype == "monolingualtext":
            kwargs["text"] = value
            kwargs.pop("value")
            return MonolingualText(**kwargs)
        elif datatype == "musical-notation":
            return MusicalNotation(**kwargs)
        elif datatype == "wikibase-property":
            return Property(**kwargs)
        elif datatype == "quantity":
            kwargs["amount"] = value
            kwargs.pop("value")
            return Quantity(**kwargs)
        elif datatype == "wikibase-sense":
            return Sense(**kwargs)
        elif datatype == "string":
            return String(**kwargs)
        elif datatype == "tabular-data":
            return TabularData(**kwargs)
        elif datatype == "time":
            kwargs["time"] = value
            kwargs.pop("value")
            return Time(**kwargs)
        elif datatype == "url":
            return URL(**kwargs)
        elif datatype == "mathml":
            return MathML(**kwargs)
        else:
            # Handle unexpected datatype
            raise ValueError(f"Unsupported datatype: {datatype}")

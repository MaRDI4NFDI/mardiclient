"""Utilities for merging and disambiguating MaRDI entities."""

from __future__ import annotations

from typing import Any

import requests
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import merge_items
from wikibaseintegrator.wbi_login import LoginError

from .mardi_config import config
from .mardi_entities import MardiItem, MardiProperty


class WBAPIException(BaseException):
    """Exception raised when the Wikibase API returns an error."""


class MardiDisambiguator(WikibaseIntegrator):
    """Client for merging and disambiguating duplicate MaRDI entities."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the disambiguator.

        Args:
            user: Username for authentication
            password: Password for authentication
            login_with_bot: Whether to use bot login (default: False)
        """
        super().__init__(is_bot=True)
        self.login = self._config(**kwargs)
        self.session = self._get_session(**kwargs)
        self.item = MardiItem(api=self)
        self.property = MardiProperty(api=self)

    @staticmethod
    def _config(
        user: str, password: str, login_with_bot: bool = False
    ) -> wbi_login.Login | wbi_login.Clientlogin | None:
        """Configure the Wikibase integrator settings.

        Args:
            user: Username for authentication
            password: Password for authentication
            login_with_bot: Whether to use bot login

        Returns:
            Login object if successful, None otherwise
        """
        wbi_config["MEDIAWIKI_API_URL"] = config["MEDIAWIKI_API_URL"]
        wbi_config["SPARQL_ENDPOINT_URL"] = config["SPARQL_ENDPOINT_URL"]
        wbi_config["WIKIBASE_URL"] = config["WIKIBASE_URL"]

        try:
            if login_with_bot:
                return wbi_login.Login(user=user, password=password)
            else:
                return wbi_login.Clientlogin(user=user, password=password)
        except LoginError:
            print("Wrong credentials")
            return None

    @staticmethod
    def _get_session(user: str, password: str) -> requests.Session:
        """Create and authenticate a session with the MediaWiki API.

        Args:
            user: Username for authentication
            password: Password for authentication

        Returns:
            Authenticated session

        Raises:
            WBAPIException: If login fails
        """
        session = requests.Session()

        # Get login token
        r1 = session.get(
            config["MEDIAWIKI_API_URL"],
            params={"format": "json", "action": "query", "meta": "tokens", "type": "login"},
            timeout=30,
        )
        r1.raise_for_status()

        # Login with credentials
        r2 = session.post(
            config["MEDIAWIKI_API_URL"],
            data={
                "format": "json",
                "action": "login",
                "lgname": user,
                "lgpassword": password,
                "lgtoken": r1.json()["query"]["tokens"]["logintoken"],
            },
            timeout=30,
        )
        r2.raise_for_status()

        if r2.json()["login"]["result"] != "Success":
            raise WBAPIException(r2.json()["login"])

        return session

    def get_csrf_token(self) -> str:
        """Get a CSRF token for authenticated requests.

        Returns:
            CSRF token string
        """
        params = {"action": "query", "meta": "tokens", "type": "csrf", "format": "json"}
        response = self.session.get(config["MEDIAWIKI_API_URL"], params=params, timeout=30)
        response.raise_for_status()
        token: str = response.json()["query"]["tokens"]["csrftoken"]
        return token

    def get_page(self, target: str, profile: str) -> bool:
        """Check if a page exists.

        Args:
            target: Page identifier
            profile: Page profile/namespace

        Returns:
            True if page exists, False otherwise
        """
        target = f"{profile}:{target}"
        params = {"action": "parse", "page": {target}, "prop": "wikitext", "format": "json"}
        response = self.session.get(config["MEDIAWIKI_API_URL"], params=params, timeout=30)
        return "error" not in response.json()

    def delete_page(self, target: str, profile: str) -> None:
        """Delete a page.

        Args:
            target: Page identifier
            profile: Page profile/namespace

        Raises:
            WBAPIException: If deletion fails
        """
        token = self.get_csrf_token()
        target = f"{profile}:{target}"

        params = {
            "action": "delete",
            "format": "json",
            "title": target,
            "token": token,
            "reason": "Duplicate",
        }

        response = self.session.post(config["MEDIAWIKI_API_URL"], data=params, timeout=30)
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            raise WBAPIException(result["error"])

    def move_page(self, source: str, target: str, profile: str) -> None:
        """Move a page from source to target.

        Args:
            source: Source page identifier
            target: Target page identifier
            profile: Page profile/namespace

        Raises:
            WBAPIException: If move fails
        """
        token = self.get_csrf_token()
        target = f"{profile}:{target}"
        source = f"{profile}:{source}"

        params = {
            "action": "move",
            "format": "json",
            "from": source,
            "to": target,
            "token": token,
            "reason": "Duplicate",
        }

        response = self.session.post(config["MEDIAWIKI_API_URL"], data=params, timeout=30)
        response.raise_for_status()
        result = response.json()

        if "error" in result:
            raise WBAPIException(result["error"])

    def merge_authors(self, source_QID: str, target_QID: str) -> tuple[str, str]:
        """Merge two author entities.

        The function determines which entity to keep based on label length
        and format (preferring comma-separated names).

        Args:
            source_qid: QID of source author
            target_qid: QID of target author

        Returns:
            Tuple of (from_qid, to_qid) after merge
        """
        source_item = self.item.get(entity_id=source_QID)
        target_item = self.item.get(entity_id=target_QID)

        source_label, target_label = "", ""

        source_dict = source_item.labels.get_json()
        target_dict = target_item.labels.get_json()
        if "en" in source_dict:
            source_label = source_dict["en"]["value"]
        if "en" in target_dict:
            target_label = target_dict["en"]["value"]

        # Swap if target has shorter label
        if len(target_label) < len(source_label):
            source_QID, target_QID = target_QID, source_QID
            source_label, target_label = target_label, source_label

        # Prefer comma-separated format (Last, First)
        if "," in target_label and source_label and "," not in source_label:
            source_QID, target_QID = target_QID, source_QID
            source_label, target_label = target_label, source_label

        source_author_id = source_QID.replace("Q", "")
        target_author_id = target_QID.replace("Q", "")

        # Redirect profile page, if it exists
        if source_label:
            # Delete target Person page
            self.delete_page(target_author_id, "Person")

            # Move source Page to target Page
            self.move_page(source_author_id, target_author_id, "Person")

        if not source_label and not target_label:
            source_page_exists = self.get_page(source_author_id, "Person")
            target_page_exists = self.get_page(target_author_id, "Person")

            if source_page_exists and not target_page_exists:
                source_QID, target_QID = target_QID, source_QID
            elif source_page_exists and target_page_exists:
                self.delete_page(target_author_id, "Person")
                self.move_page(source_author_id, target_author_id, "Person")

        # Merge items
        results = merge_items(source_QID, target_QID, login=self.login, is_bot=True)
        return results["from"]["id"], results["to"]["id"]

    def merge_publications(self, source_QID: str, target_QID: str) -> tuple[str, str]:
        """Merge two publication entities.

        Args:
            source_qid: QID of source publication
            target_qid: QID of target publication

        Returns:
            Tuple of (from_qid, to_qid) after merge
        """
        source_publication_id = source_QID.replace("Q", "")
        target_publication_id = target_QID.replace("Q", "")

        # Harmonize description before merging
        source_item = self.item.get(entity_id=source_QID)
        source_claims = source_item.claims.get_json()

        target_item = self.item.get(entity_id=target_QID)
        target_claims = target_item.claims.get_json()

        zbmath_prop = "P1451"
        if source_claims.get(zbmath_prop):
            zbmath_de_number = source_claims.get(zbmath_prop)[0]["mainsnak"]["datavalue"]["value"]
            description = f"scientific article; zbMATH DE number {zbmath_de_number}"
            source_item.descriptions.set(language="en", value=description)
            source_item.write()

            target_item.descriptions.values["en"].remove()
            target_item.write()
        elif target_claims.get(zbmath_prop):
            zbmath_de_number = target_claims.get(zbmath_prop)[0]["mainsnak"]["datavalue"]["value"]
            description = f"scientific article; zbMATH DE number {zbmath_de_number}"
            target_item.descriptions.set(language="en", value=description)
            target_item.write()

            source_item.descriptions.values["en"].remove()
            source_item.write()

        # Delete target Publication page
        self.delete_page(target_publication_id, "Publication")

        # Move source Page to target Page
        self.move_page(source_publication_id, target_publication_id, "Publication")

        results = merge_items(source_QID, target_QID, login=self.login, is_bot=True)
        return results["from"]["id"], results["to"]["id"]

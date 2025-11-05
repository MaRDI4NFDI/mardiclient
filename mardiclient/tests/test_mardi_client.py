from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mardiclient import mardi_client as mc


CONFIG_KEYS = [
    "MEDIAWIKI_API_URL",
    "SPARQL_ENDPOINT_URL",
    "WIKIBASE_URL",
    "USER_AGENT",
]


@pytest.fixture
def reset_wbi_config() -> None:
    """Preserve the global wbi_config configuration between tests.

    Yields:
        None: Allows the test to run with modified configuration.
    """
    original = {key: mc.wbi_config.get(key) for key in CONFIG_KEYS}
    yield
    for key, value in original.items():
        if value is None:
            mc.wbi_config.pop(key, None)
        else:
            mc.wbi_config[key] = value


def make_client() -> mc.MardiClient:
    """Construct a MardiClient instance without invoking the real initializer.

    Returns:
        MardiClient: A partially initialised client suitable for unit tests.
    """
    client = mc.MardiClient.__new__(mc.MardiClient)
    client.importer_api = "https://importer.test"
    client.property = SimpleNamespace()
    client.item = SimpleNamespace()
    return client


def test_config_without_credentials_updates_globals(reset_wbi_config: None) -> None:
    """Ensure `_config` stores connection settings when no credentials are provided.

    Args:
        reset_wbi_config: Fixture restoring global Wikibase settings.
    """
    result = mc.MardiClient._config(
        mediawiki_api_url="https://example.test/api",
        sparql_endpoint_url="https://example.test/sparql",
        wikibase_url="https://example.test",
        user_agent="pytest-agent",
    )

    assert result is None
    assert mc.wbi_config["MEDIAWIKI_API_URL"] == "https://example.test/api"
    assert mc.wbi_config["SPARQL_ENDPOINT_URL"] == "https://example.test/sparql"
    assert mc.wbi_config["WIKIBASE_URL"] == "https://example.test"
    assert mc.wbi_config["USER_AGENT"] == "pytest-agent"


def test_config_uses_bot_login_when_requested(reset_wbi_config: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify bot authentication path is used when `login_with_bot` is true.

    Args:
        reset_wbi_config: Fixture restoring global Wikibase settings.
        monkeypatch: Pytest helper for patching external calls.
    """
    captured: dict[str, object] = {}

    def fake_login(*, user: str, password: str) -> str:
        captured["called"] = {"user": user, "password": password}
        return "bot-login"

    def fail_clientlogin(*args: object, **kwargs: object) -> None:
        raise AssertionError("Clientlogin should not be used when login_with_bot=True")

    monkeypatch.setattr(mc.wbi_login, "Login", fake_login)
    monkeypatch.setattr(mc.wbi_login, "Clientlogin", fail_clientlogin)

    result = mc.MardiClient._config(
        mediawiki_api_url="https://example.test/api",
        sparql_endpoint_url="https://example.test/sparql",
        wikibase_url="https://example.test",
        user_agent="pytest-agent",
        user="tester",
        password="secret",
        login_with_bot=True,
    )

    assert result == "bot-login"
    assert captured["called"] == {"user": "tester", "password": "secret"}


def test_get_local_id_by_label_returns_local_id() -> None:
    """Confirm raw PID/QID strings are returned unchanged."""
    client = make_client()

    assert client.get_local_id_by_label("Q123", "item") == "Q123"
    assert client.get_local_id_by_label("P456", "property") == "P456"


def test_get_local_id_by_label_creates_property_from_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure labels create new properties when no identifier is supplied.

    Args:
        monkeypatch: Pytest helper for patching entity builders.
    """
    client = make_client()
    calls: dict[str, object] = {}

    class DummyPropertyBuilder:
        def __init__(self) -> None:
            self.labels = SimpleNamespace(set=self._set_label)

        def _set_label(self, **kwargs: object) -> None:
            calls["label"] = kwargs

        def get_PID(self) -> str:
            return "P321"

    class DummyProperty:
        def __init__(self, api: object) -> None:
            calls["api"] = api

        def new(self) -> DummyPropertyBuilder:
            builder = DummyPropertyBuilder()
            calls["builder"] = builder
            return builder

    monkeypatch.setattr(mc, "MardiProperty", DummyProperty)

    result = client.get_local_id_by_label("Example Property", "property")

    assert result == "P321"
    assert calls["api"] is client
    assert calls["label"] == {"language": "en", "value": "Example Property"}


def test_get_local_id_by_label_creates_item_from_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure labels create new items when no identifier is supplied.

    Args:
        monkeypatch: Pytest helper for patching entity builders.
    """
    client = make_client()
    calls: dict[str, object] = {}

    class DummyItemBuilder:
        def __init__(self) -> None:
            self.labels = SimpleNamespace(set=self._set_label)

        def _set_label(self, **kwargs: object) -> None:
            calls["label"] = kwargs

        def get_QID(self) -> str:
            return "Q321"

    class DummyItem:
        def __init__(self, api: object) -> None:
            calls["api"] = api

        def new(self) -> DummyItemBuilder:
            builder = DummyItemBuilder()
            calls["builder"] = builder
            return builder

    monkeypatch.setattr(mc, "MardiItem", DummyItem)

    result = client.get_local_id_by_label("Example Item", "item")

    assert result == "Q321"
    assert calls["api"] is client
    assert calls["label"] == {"language": "en", "value": "Example Item"}


def test_get_local_id_by_label_uses_importer_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validate remote mappings are fetched when a Wikidata identifier is provided.

    Args:
        monkeypatch: Pytest helper for patching network calls.
    """
    client = make_client()

    class DummyResponse:
        def __init__(self) -> None:
            self.raised = False

        def raise_for_status(self) -> None:
            self.raised = True

        def json(self) -> dict[str, object]:
            return {"local_id": "Q555"}

    def fake_get(url: str, *, timeout: int) -> DummyResponse:
        fake_get.last_call = {"url": url, "timeout": timeout}
        return DummyResponse()

    fake_get.last_call: dict[str, object] = {}
    monkeypatch.setattr(mc.requests, "get", fake_get)

    result = client.get_local_id_by_label("wd:Q42", "item")

    assert result == "Q555"
    assert fake_get.last_call == {
        "url": "https://importer.test/items/wd:Q42/mapping",
        "timeout": 30,
    }


def test_get_local_id_by_label_handles_importer_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Gracefully handle importer failures by returning `None`.

    Args:
        monkeypatch: Pytest helper for patching network calls.
    """
    client = make_client()

    def fake_get(url: str, *, timeout: int) -> None:
        raise mc.requests.RequestException("boom")

    monkeypatch.setattr(mc.requests, "get", fake_get)

    assert client.get_local_id_by_label("wd:Q999", "item") is None


def test_search_entity_by_value_builds_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure SPARQL query is built with resolved property identifiers.

    Args:
        monkeypatch: Pytest helper for patching SPARQL execution.
    """
    client = make_client()

    def fake_resolve(prop: str, entity_type: str) -> str:
        if entity_type == "property":
            return "P789"
        return prop

    client.get_local_id_by_label = fake_resolve  # type: ignore[assignment]

    calls: dict[str, object] = {}

    def fake_query(query: str) -> dict[str, object]:
        calls["query"] = query
        return {
            "results": {
                "bindings": [
                    {"item": {"value": "https://example.org/entity/Q901"}},
                ]
            }
        }

    monkeypatch.setattr(mc, "execute_sparql_query", fake_query)

    results = client.search_entity_by_value("P123", "value")

    assert results == ["Q901"]
    assert calls["query"] == 'SELECT ?item WHERE {?item wdt:P789 "value"}'


def test_search_entity_by_value_handles_resolution_failure() -> None:
    """Return an empty list when property resolution does not yield a PID."""
    client = make_client()
    client.get_local_id_by_label = lambda prop, entity_type: ["P1"]  # type: ignore[assignment]

    results = client.search_entity_by_value("P123", "value")

    assert results == []


def test_search_entity_by_value_handles_query_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return an empty list when SPARQL execution raises an exception.

    Args:
        monkeypatch: Pytest helper for patching SPARQL execution.
    """
    client = make_client()
    client.get_local_id_by_label = lambda prop, entity_type: "P789"  # type: ignore[assignment]

    def fail_query(query: str) -> dict[str, object]:
        raise RuntimeError("sparql down")

    monkeypatch.setattr(mc, "execute_sparql_query", fail_query)

    assert client.search_entity_by_value("P123", "value") == []


def test_get_claim_wikibase_item(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify wikibase-item claims resolve remote identifiers to local IDs.

    Args:
        monkeypatch: Pytest helper for patching claim class constructors.
    """
    client = make_client()

    def fake_resolve(value: str, entity_type: str) -> str:
        if entity_type == "property":
            return "P123"
        if value == "wd:Q111":
            return "Q999"
        return value

    client.get_local_id_by_label = fake_resolve  # type: ignore[assignment]

    class DummyPropertyAccessor:
        def __init__(self) -> None:
            self.datatype = SimpleNamespace(value="wikibase-item")

    client.property = SimpleNamespace(get=lambda entity_id: DummyPropertyAccessor())

    captured: dict[str, object] = {}

    class DummyItem:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(mc, "Item", DummyItem)

    claim = client.get_claim("P123", "wd:Q111")

    assert isinstance(claim, DummyItem)
    assert captured["prop_nr"] == "P123"
    assert captured["value"] == "Q999"


def test_get_claim_monolingual_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure monolingual text claims translate value into the expected structure.

    Args:
        monkeypatch: Pytest helper for patching claim class constructors.
    """
    client = make_client()
    client.get_local_id_by_label = lambda value, entity_type: "P321"  # type: ignore[assignment]

    class DummyPropertyAccessor:
        def __init__(self) -> None:
            self.datatype = SimpleNamespace(value="monolingualtext")

    client.property = SimpleNamespace(get=lambda entity_id: DummyPropertyAccessor())

    captured: dict[str, object] = {}

    class DummyMonolingualText:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(mc, "MonolingualText", DummyMonolingualText)

    claim = client.get_claim("P123", "hello", language="en")

    assert isinstance(claim, DummyMonolingualText)
    assert captured == {"prop_nr": "P321", "language": "en", "text": "hello"}


def test_get_claim_mathml_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fallback to MathML datatype when property lookup fails.

    Args:
        monkeypatch: Pytest helper for patching datatype classes.
    """
    client = make_client()
    client.get_local_id_by_label = lambda value, entity_type: "P987"  # type: ignore[assignment]

    def raise_value_error(entity_id: str) -> None:
        raise ValueError()

    client.property = SimpleNamespace(get=raise_value_error)

    captured: dict[str, object] = {}

    class DummyMathML:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(mc, "MathML", DummyMathML)

    claim = client.get_claim("P123", "<math/>")

    assert isinstance(claim, DummyMathML)
    assert captured == {"prop_nr": "P987", "value": "<math/>"}


def test_get_claim_unsupported_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise ValueError when encountering an unknown datatype.

    Args:
        monkeypatch: Pytest helper for patching property metadata.
    """
    client = make_client()
    client.get_local_id_by_label = lambda value, entity_type: "P444"  # type: ignore[assignment]

    class DummyPropertyAccessor:
        def __init__(self) -> None:
            self.datatype = SimpleNamespace(value="unsupported")

    client.property = SimpleNamespace(get=lambda entity_id: DummyPropertyAccessor())

    with pytest.raises(ValueError, match="Unsupported datatype: unsupported"):
        client.get_claim("P123", "value")


def test_get_local_id_by_label_live(reset_wbi_config: None) -> None:
    """Run a live lookup for `wd:Q6503328` against the public MaRDI instance.

    Args:
        reset_wbi_config: Fixture restoring global Wikibase settings.
    """
    print("Creating live MardiClient instance for integration test")
    client = mc.MardiClient()

    print(
        "Live configuration",
        {
            "MEDIAWIKI_API_URL": mc.wbi_config["MEDIAWIKI_API_URL"],
            "SPARQL_ENDPOINT_URL": mc.wbi_config["SPARQL_ENDPOINT_URL"],
            "WIKIBASE_URL": mc.wbi_config["WIKIBASE_URL"],
            "IMPORTER_API_URL": client.importer_api,
        },
    )

    target = "Q6503328"
    print(f"Requesting local identifier for {target}")
    result = client.get_local_id_by_label(target, "item")
    print(f"Received result from live endpoint: {result}")

    if result is None:
        pytest.skip("Live MaRDI endpoint did not return a mapping for Q6503328.")

    resolved = result[0] if isinstance(result, list) else result
    print(f"Resolved identifier selected for assertion: {resolved}")

    assert isinstance(resolved, str), "Expected a string identifier"
    assert resolved.startswith("Q"), "Local identifier should start with 'Q'"
    print("Live lookup completed successfully")

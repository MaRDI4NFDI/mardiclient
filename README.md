# mardi-client
MaRDI client to interact with the MaRDI knowledge graph.

## Installation

```bash
pip install git+https://github.com/MaRDI4NFDI/mardiclient.git
```

## Setup user and password
```python
from mardiclient import MardiClient

mc = MardiClient(user="username", password="my-password")
```

## Get data from an existing item
```python
my_item = mc.item.get(entity_id='Q1')
```

## Create a new item with statements
```python
# Create a new item
item = mc.item.new()

# Set an english label
item.labels.set(language='en', value='My package')

# Set an english description
item.descriptions.set(language='en', value='A generic R package')

# Add a statement (instance of = R package)
item.add_claim('wdt:P31', 'wd:Q73539779')

# Write the item
item.write()
```

Wikidata properties and items must be prefixed with ```wdt:``` and ```wd:```, respectively.
No prefixes are necessary if MaRDI identifiers are used.

## Change default configuration
The MaRDI Client is setup to interact with the portal at https://portal.mardi4nfdi.de

The default settings can be overwritten with
```python
from mardiclient import config

config['IMPORTER_API_URL'] = 'api_endpoint'
config['MEDIAWIKI_API_URL'] = 'mediawiki_api'
config['SPARQL_ENDPOINT_URL'] = 'sparql_endpoint'
config['WIKIBASE_URL'] = 'wikibase_url'
```
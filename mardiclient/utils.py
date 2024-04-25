import requests

from .mardi_config import config
from .MardiEntities import MardiItem, MardiProperty
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import merge_items
from wikibaseintegrator.wbi_login import LoginError

class MardiDisambiguator(WikibaseIntegrator):
    def __init__(self, **kwargs) -> None:
        super().__init__(is_bot=True)
        self.login = self.config(**kwargs)
        self.session = self.get_session(**kwargs)
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

    @staticmethod
    def get_session(user, password):
        """
        Starts a new session and logins using a bot account.
        @username, @botpwd string: credentials of an existing bot user
        @returns requests.sessions.Session object
        """
        # create a new session
        session = requests.Session()

        # get login token
        r1 = session.get(config['MEDIAWIKI_API_URL'], params={
            'format': 'json',
            'action': 'query',
            'meta': 'tokens',
            'type': 'login'
        })
        # login with bot account
        r2 = session.post(config['MEDIAWIKI_API_URL'], data={
            'format': 'json',
            'action': 'login',
            'lgname': user,
            'lgpassword': password,
            'lgtoken': r1.json()['query']['tokens']['logintoken'],
        })
        # raise when login failed
        if r2.json()['login']['result'] != 'Success':
            raise WBAPIException(r2.json()['login'])
            
        return session
    
    def get_csrf_token(self):
        """Gets a security (CSRF) token."""
        params = {
            "action": "query",
            "meta": "tokens",
            "type": "csrf",
            "format": "json"
        }
        r1 = self.session.get(config['MEDIAWIKI_API_URL'], params=params)
        token = r1.json()['query']['tokens']['csrftoken']

        return token

    def get_page(self, target, profile):
        target = f"{profile}:{target}"
        params = {
            "action": "parse",
            "page": {target},
            "prop": "wikitext",
            "format": "json"
        }
        r1 = self.session.get(config['MEDIAWIKI_API_URL'], params=params)
        if 'error' in r1.json().keys():
            return False
        return True
    
    def delete_page(self, target, profile):
        token = self.get_csrf_token()

        target = f"{profile}:{target}"
        
        params = {
            "action": "delete",
            "format": "json",
            "title": target,
            "token": token,
            "reason": "Duplicate"
        }
        r1 = self.session.post(config['MEDIAWIKI_API_URL'], data=params)
        r1.json = r1.json()
        
        if 'error' in r1.json.keys():
            raise WBAPIException(r1.json['error'])
        
    def move_page(self, source, target, profile):
        token = self.get_csrf_token()

        target = f"{profile}:{target}"
        source = f"{profile}:{source}"
        
        params = {
            "action": "move",
            "format": "json",
            "from": source,
            "to": target,
            "token": token,
            "reason": "Duplicate"
        }
        r1 = self.session.post(config['MEDIAWIKI_API_URL'], data=params)
        r1.json = r1.json()
        
        if 'error' in r1.json.keys():
            raise WBAPIException(r1.json['error'])

    def merge_authors(self, source_QID, target_QID):
        source_item = self.item.get(entity_id=source_QID)
        target_item = self.item.get(entity_id=target_QID)

        source_label, target_label = "", ""

        source_dict = source_item.labels.get_json()
        target_dict = target_item.labels.get_json()
        if 'en' in source_dict.keys():
            source_label = source_dict['en']['value']
        if 'en' in target_dict.keys():
            target_label = target_dict['en']['value']
        
        if len(target_label) < len(source_label):
            source_QID, target_QID = target_QID, source_QID
            source_label, target_label = target_label, source_label

        if ',' in target_label and source_label and ',' not in source_label:
            source_QID, target_QID = target_QID, source_QID
            source_label, target_label = target_label, source_label

        source_author_id = source_QID.replace('Q', '')
        target_author_id = target_QID.replace('Q', '')

        # Redirect profile page, if it exists
        if source_label:
            # Delete target Person page
            self.delete_page(target_author_id, 'Person')

            # Move source Page to target Page
            self.move_page(source_author_id, target_author_id, 'Person')

        if not source_label and not target_label:

            source_page_exists = self.get_page(source_author_id, 'Person')
            target_page_exists = self.get_page(target_author_id, 'Person')

            if source_page_exists and not target_page_exists:
                source_QID, target_QID = target_QID, source_QID
            elif source_page_exists and target_page_exists:
                self.delete_page(target_author_id, 'Person')
                self.move_page(source_author_id, target_author_id, 'Person')

        # Merge items
        results = merge_items(source_QID, target_QID, login=self.login, is_bot=True)
        return results['from']['id'], results['to']['id']

    def merge_publications(self, source_QID, target_QID):
        source_publication_id = source_QID.replace('Q', '')
        target_publication_id = target_QID.replace('Q', '')

        # Delete target Publication page
        self.delete_page(target_publication_id, 'Publication')

        # Move source Page to target Page
        self.move_page(source_publication_id, target_publication_id, 'Publication')

        results = merge_items(source_QID, target_QID, login=self.login, is_bot=True)
        return results['from']['id'], results['to']['id']


class WBAPIException(BaseException):
    """Raised when the wikibase Open API throws an error"""
    pass
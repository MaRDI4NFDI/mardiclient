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
    
    def delete_page(self, target):
        token = self.get_csrf_token()

        target = f"Person:{target}"
        
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
        
    def move_page(self, source, target):
        token = self.get_csrf_token()

        target = f"Person:{target}"
        source = f"Person:{source}"
        
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

        source_label = source_item.labels.get_json()['en']['value']
        target_label = target_item.labels.get_json()['en']['value']
        
        if len(target_label) < len(source_label):
            source_QID, target_QID = target_QID, source_QID

        source_author_id = source_QID.replace('Q', '')
        target_author_id = target_QID.replace('Q', '')

        # Delete target Person page
        self.delete_page(target_author_id)

        # Move source Page to target Page
        self.move_page(source_author_id, target_author_id)

        # Merge items
        results = merge_items(source_QID, target_QID, login=self.login, is_bot=True)
        return results['from']['id'], results['to']['id'] 


class WBAPIException(BaseException):
    """Raised when the wikibase Open API throws an error"""
    pass
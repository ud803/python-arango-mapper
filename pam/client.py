import logging
from arango import ArangoClient
from arango.http import HTTPClient
from arango.response import Response
from pam import database 
from requests import Session
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class CustomHTTPClient(HTTPClient):
    def __init__(self):
        self._logger = logging.getLogger('my_logger')

    def create_session(self, host):
        session = Session()

        # Add request header.
        session.headers.update({'x-my-header': 'true'})

        # Enable retries.
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('https://', http_adapter)
        session.mount('http://', http_adapter)

        return session

    def send_request(self,
                     session,
                     method,
                     url,
                     params=None,
                     data=None,
                     headers=None,
                     auth=None):
        # Add your own debug statement.
        self._logger.debug(f'Sending request to {url}')

        # Send a request.
        response = session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=headers,
            auth=auth,
            timeout=600
        )
        self._logger.debug(f'Got {response.status_code}')

        # Return an instance of arango.response.Response.
        return Response(
            method=response.request.method,
            url=response.url,
            headers=response.headers,
            status_code=response.status_code,
            status_text=response.reason,
            raw_body=response.text,
        )


def get_arango_conn(hosts):
    """Returns ArangoDB Client Connection Object
    
    :parameters:
    - `hosts`: host of ArangoDB (string)
    """

    arango_conn = ArangoClient(
        hosts=hosts,
        http_client=CustomHTTPClient()
    )

    return arango_conn


'''
Thin wrapper around a Confluent-compatible Schema Registry fronted by Okta
OAuth client credentials. Used by `identity_raw.py` to fetch Avro schemas by
version.
'''
import requests


class SchemaRegistry:
    def __init__(self, oauth_endpoint: str, client_id: str, client_secret: str,
                 grant_type: str, registry_url: str, subject_name: str):
        self.oauth_endpoint = oauth_endpoint
        self.client_id      = client_id
        self.client_secret  = client_secret
        self.grant_type     = grant_type
        self.registry_url   = registry_url.rstrip("/")
        self.subject_name   = subject_name
        self._token = None

    def _get_token(self) -> str:
        if self._token:
            return self._token
        resp = requests.post(self.oauth_endpoint,
                             data={"grant_type": self.grant_type,
                                   "client_id":  self.client_id,
                                   "client_secret": self.client_secret})
        resp.raise_for_status()
        self._token = resp.json()["access_token"]
        return self._token

    def get_schema_for_given_version(self, version: int) -> str:
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        url = f"{self.registry_url}/subjects/{self.subject_name}/versions/{version}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()["schema"]

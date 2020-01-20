from urllib.parse import urlparse

import requests


class Miniq:
    def __init__(self, hostname):
        url = urlparse(hostname)
        if url.scheme and url.netloc:
            uri = url.scheme + '://' + url.netloc
        else:
            uri = 'http://' + url.path
        self.uri = uri

    def create_job(self, key, data):
        response = requests.post(self.uri + '/{}'.format(int(key)), data=data)
        response.raise_for_status()
        return response.json()['data']

    def get_queueset(self):
        response = requests.get(self.uri)
        response.raise_for_status()
        return response.json()['data']

    def get_queue(self, key):
        response = requests.get(self.uri + '/{}'.format(key))
        response.raise_for_status()
        return response.json()['data']

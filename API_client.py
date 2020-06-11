from time import sleep

import requests as rq
import re

prefix = 'http://localhost:9875'


def print_response(response, body=None):
    print('Request:')
    print('\tUrl: {}'.format(response.url))
    print('\tMethod: {}'.format(re.search("<PreparedRequest \[(\w+)\]>", str(response.request)).group(1)))
    print('\tBody: {}'.format(body))

    print('Response:')
    print('\tCode: {}'.format(response.status_code))
    content = response.content.decode("utf-8")
    if len(content) > 200:
        content = content[:160] + "..." + content[-38:]
    print('\tContent: {}'.format(content), end='')
    print('\tHeaders: {}'.format(response.headers))
    print('-' * 30)


def send_get(message, url):
    print(message)
    url = prefix + url
    response = rq.get(url)
    print_response(response)


def send_post(message, url, body=None):
    print(message)
    url = prefix + url
    response = rq.post(url, json=body)
    print_response(response, body)


# ------ Simple operations ------
send_get('Get rating ', '/rating/75/3')
send_get('Document for user with ID = 75', '/user/document/75')
send_get('Non existing user document', '/user/document/0')
send_get('Document for movie with ID = 3', '/movie/document/2')

# ------ Preselection ------
send_get('Preselection for user 75', '/user/preselection/75')
send_get('Preselection for movie 3', '/movie/preselection/2')

# ------ Index test ------
send_get('Get all index', '/index/all')

# ------ Generation ------
while True:
    send_get('Generated data:', '/generator')
    sleep(15)

import json

def parse_options():
    return parse_file('./powa-remote.conf')

def parse_file(filepath):
    return json.load(open(filepath))

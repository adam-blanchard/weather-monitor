import json

def get_config():
    config = {}
    with open('config.json', 'r') as f:
        config = json.loads(f.read())
    return config

import sys, os
import json

SCRIPT_DIR = os.path.dirname(__file__)
try:
    with open(os.path.join(SCRIPT_DIR, 'config.json'), 'r') as f:
        config = json.load(f)
except:
    raise Exception('Cannot load configuration file')
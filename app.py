import json
from jos import *

query = {
    "query": {
        "term": {
            "city": "jerusalem"
        }
    }
}

print(json.dumps(substitute(query['query'], join_on)))


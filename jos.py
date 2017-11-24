from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

compound = ['bool']

join_on = 'city'
join_by = 'zip'

join_from_index = 'cityzip'
main_index = 'people'

url = 'localhost:9200'

es_client = Elasticsearch(url)


def substitute(clause, join_on):
    builder = {}
    for sub_clause in clause.keys():
        if sub_clause in compound:
            builder[sub_clause] = substitute(sub_clause)
        if querys_on(clause[sub_clause], join_on):
            builder['bool'] = turn_to_should(sub_clause, clause[sub_clause])
    return builder


def querys_on(clause, field):
    does_it = field in clause.keys()
    return does_it


def turn_to_should(clause, sub):
    return {
        "should": [{clause: sub}, get_join_query(clause, sub)]
    }


def get_join_query(clause, sub):
    s = Search(using=es_client, index=join_from_index) \
        .filter(clause, **sub)

    response = s.execute()

    joiners = [hit[join_by] for hit in response]
    return {
        "terms": {
            join_by: joiners
        }
    }
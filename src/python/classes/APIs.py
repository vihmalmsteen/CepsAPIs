import requests
from pprint import pprint
import pandas as pd
from tabulate import tabulate

# cep = '22793395'
# brasilapi = 'https://brasilapi.com.br/api/cep/v2/{}'.format(cep)
# cep_get = requests.get(f'https://brasilapi.com.br/api/cep/v2/{cep}').json()

results_json = {}

my_request_json = {
    'cep': '22793395','city': 'Rio de Janeiro',
    'location': {
        'coordinates': {}, 
        'type': 'Point'
        },
    'neighborhood': 'Barra da Tijuca',
    'service': 'open-cep',
    'state': 'RJ',
    'street': 'Rua Sylvio da Rocha Pollis'
    }
cep_get = my_request_json

# pprint(my_request_json)
results_json['service'] = 'brasilapi_' + my_request_json.get('service')
results_json['pais'] = 'Brasil'
results_json['UF'] = my_request_json.get('state')
results_json['cidade'] = my_request_json.get('city')
results_json['bairro'] = my_request_json.get('neighborhood')
results_json['logradouro'] = my_request_json.get('street')
results_json['CEP'] = my_request_json.get('cep')

results_df = pd.DataFrame.from_dict(results_json, orient='index').T
print(results_df)


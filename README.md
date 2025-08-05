# FALTA

* Fazer a consulta nas APIs:
  * 'brasilapi v1': "https://brasilapi.com.br/api/cep/v1/{cep}" (FEITO)
  * 'brasilapi v2': "https://brasilapi.com.br/api/cep/v2/{cep}"
  * 'viacep': "https://viacep.com.br/ws/{cep}/json/"
  * 'widenet': "https://cdn.apicep.com/file/apicep/{cep}.json"
  
  ```json5
  // brasilapi v1
  {
  "cep": "89010025",
  "state": "SC",
  "city": "Blumenau",
  "neighborhood": "Centro",
  "street": "Rua Doutor Luiz de Freitas Melro",
  "service": "viacep"
  }

  // brasilapi v2
  {
  "cep": "89010025",
  "state": "SC",
  "city": "Blumenau",
  "neighborhood": "Centro",
  "street": "Rua Doutor Luiz de Freitas Melro",
  "service": "viacep",
  "location": {
    "type": "Point",
    "coordinates": {
      "longitude": "-49.0629788",
      "latitude": "-26.9244749"
      }
    }
  }
  
  // viacep
  {
  "cep": "01001-000",
  "logradouro": "Praça da Sé",
  "complemento": "lado ímpar",
  "unidade": "",
  "bairro": "Sé",
  "localidade": "São Paulo",
  "uf": "SP",
  "estado": "São Paulo",
  "regiao": "Sudeste",
  "ibge": "3550308",
  "gia": "1004",
  "ddd": "11",
  "siafi": "7107"
  }

  // widenet
  {
  "status":200,
  "code":"06233-030",
  "state":"SP",
  "city":"Osasco",
  "district":"Piratininga",
  "address":"Rua Paula Rodrigues"
  }


  ```

* Fazer method em DBDataclass para JOIN entre o retorno do JS (UF, cidade, bairro...) e participantes.



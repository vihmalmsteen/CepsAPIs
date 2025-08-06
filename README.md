# APIs CEPs to Addresses

Para atender a demanda de CEPs do job. Busca em 3 APIs gratuitas (com `fallbacks`) os endereços com base no CEP.

* [brasilapi](https://brasilapi.com.br/)
* [viacep](https://viacep.com.br/)
* [apicep](https://apicep.com/api-de-consulta/)

### Requisitos:

* [Python 3.10](https://www.python.org/downloads/release/python-3100/)
* [uv pkg manager](https://docs.astral.sh/uv/getting-started/installation/#installation-methods)
* [Extensão do Jupyter no VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)


No terminal:

```
git clone https://github.com/vihmalmsteen/ceps.git
uv sync
```

### Do projeto:

```
ceps/
├─ .vscode/
│  └─ settings.json
├─ src/
│  ├─ datasets/
│  ├─ python/
│  │  ├─ classes/
│  │  │  ├─ CallsClass.py
│  │  │  └─ DBData.py
│  │  └─ main.py
│  └─ queries/
│     ├─ select_ceps.sql
│     ├─ select_editions.sql
│     └─ select_participants.sql
├─ .env-example
├─ .gitignore
├─ .python-version
├─ main.ipynb
├─ pyproject.toml
├─ README.md
└─ uv.lock
```

Arquivo principal é o `main.ipynb`. Basta executar as células do notebook e prosseguir conforme orientação. Fluxo:

* Consulta o DB e retorna as bases, exportando-as para `datasets`;
* Chama as APIs com `fallbacks` entre elas;
* Une as informações e exporta para `datasets/results`

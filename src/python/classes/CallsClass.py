import os
from time import sleep
from dotenv import load_dotenv
import requests
import pandas as pd
from tqdm import tqdm
from IPython.display import clear_output
from tabulate import tabulate
from pprint import pprint


load_dotenv()


class CallsClass:
    '''
    Classe que realiza as chamadas aos APIs.\n
    ## Atributos\n
    * **api1:** URL da API 1.\n
    * **api2:** URL da API 2.\n
    * **api3:** URL da API 3.\n
    * **api4:** URL da API 4.\n
    * **google_api_key:** Chave da API do Google.\n
    * **path_to_forms:** Caminho para o diretório com os arquivos de formulário.\n
    ## Métodos\n
    * **load_and_parse_forms:** Carrega e parseia os arquivos de formulário.\n
    * **brasilapi_one:** Chama a v1 da 'Brasil API'.\n
    * **brasilapi_two:** Chama a v2 da 'Brasil API'.\n
    * **viacep:** Chama a API da 'ViaCEP'.\n
    * **apicep:** Chama a API da 'APICEP'.\n
    * **google_geolocation:** Chama a API do 'Google Maps'.
    '''

    def __init__(self, default_timesleep:int = 2):
        self.default_timesleep = default_timesleep
        self.brasilapi_v1_url = 'https://brasilapi.com.br/api/cep/v1/replace_cep'
        self.brasilapi_v2_url = 'https://brasilapi.com.br/api/cep/v2/replace_cep'
        self.viacep_url = 'https://viacep.com.br/ws/replace_cep/json/'
        self.apicep_url = 'https://cdn.apicep.com/file/apicep/replace_cep.json'
        self.google_maps_url = 'https://maps.googleapis.com/maps/api/geocode/json?address=replace_cep&key=replace_google_key'
        self.google_api_key = os.getenv('GOOGLE_GEOLOCATION_API_KEY_FAKE')
        self.path_to_forms = os.path.join(os.getcwd(), 'src', 'datasets', 'forms')

    def load_and_parse_forms(self):
        '''
        Procura no path dos formulários arquivos excel (.xlsx).\n
        User informa qualquer um deles para parsear e carregar.\n
        Retorna um dataframe do próprio forms com as colunas: 'cep', 'parsed_ceps', 'peculiar_ceps':\n
        * **cep:** CEP do formulário.\n
        * **parsed_ceps:** CEP parseado.\n
        * **peculiar_ceps:** CEPs estranhos (que não foram possíveis de parsear).\n
        ## Das regras:
        * substitui '_' por zero
        * substitui None por ''
        * Se acima de 8 dígitos, pega os 8 primeiros
        '''
    
        # parsing forms folder files (forms)
        self.forms_files = []
        self.full_path = None
        for root, dirs, files in os.walk(self.path_to_forms):
            self.full_path = root
            for file in files:
                self.forms_files.append(file) if file.endswith('.xlsx') else None

        print('Arquivos encontrados:\n',self.forms_files)
        selected_form_file = input("Arquivo de formulário para parsear e carregar: ")

        if selected_form_file not in self.forms_files:
            print("Arquivo de formulário não encontrado.")
            return

        self.full_path = os.path.join(self.full_path, selected_form_file)

        try:
            print('\nArquivo carregado. Parseando...')
            self.df = pd.read_excel(self.full_path, engine='openpyxl')
            self.df['cep'] = self.df['cep'].astype(str)
            self.df['parsed_ceps'] = self.df['cep']
            total_ceps = len(self.df)
            ceps_with_underline = self.df['parsed_ceps'].str.count('_').sum()
            ceps_without_hyphen = (~self.df['parsed_ceps'].str.contains('-')).sum()
            self.df['parsed_ceps'] = (self.df['parsed_ceps']
                                      .replace('_', '0', regex=True)
                                      .replace('-', '', regex=True).fillna(''))
            self.df['peculiar_ceps'] = None
            
            ceps_ok = 0
            ceps_too_short = 0
            ceps_too_long = 0
            ceps_peculiar = 0
            
            for idx, row in self.df.iterrows():
                cep_val = row.loc['parsed_ceps']

                # se cep tem 8 dígitos, ok
                if len(cep_val) == 8:
                    ceps_ok += 1
                    pass

                # se cep tem menos de 8 dígitos, preencher com 0 na direita
                elif len(cep_val) < 8:
                    ceps_too_short += 1
                    filling_zeros_at_right = cep_val.ljust(8, '0')
                    self.df.loc[idx,'parsed_ceps'] = filling_zeros_at_right

                # se cep tem mais de 8 dígitos, pegar os 8 primeiros
                elif len(cep_val) > 8:
                    ceps_too_long += 1
                    picking_first_eight = cep_val[:8]
                    self.df.loc[idx,'parsed_ceps'] = picking_first_eight

                # se o cep não cabe nos anteriores, marcar com 'X'
                else:
                    ceps_peculiar += 1
                    self.df.loc[idx,'parsed_ceps'+1] = 'X'

            # logs
            ceps_logs = pd.DataFrame({
                'total de ceps': [f'{total_ceps} (100.00%)'],
                'ceps com underline': [f'{ceps_with_underline} ({ceps_with_underline/total_ceps:.2%})'],
                'ceps sem hifen': [f'{ceps_without_hyphen} ({ceps_without_hyphen/total_ceps:.2%})'],
                'ceps ok': [f'{ceps_ok} ({ceps_ok/total_ceps:.2%})'],
                'ceps com menos de 8 dígitos': [f'{ceps_too_short} ({ceps_too_short/total_ceps:.2%})'],
                'ceps com mais de 8 dígitos': [f'{ceps_too_long} ({ceps_too_long/total_ceps:.2%})'],
                'ceps com tratamento especial': [f'{ceps_peculiar} ({ceps_peculiar/total_ceps:.2%})']
            }).T.rename(columns={0: 'qtde (percent)'})

            # reinserir o traco removido
            self.df['parsed_ceps'] = self.df['parsed_ceps'].str[0:5] + '-' + self.df['parsed_ceps'].str[5:]

            print(tabulate(ceps_logs, headers='keys', tablefmt='psql'))
            print('Parseado com sucesso.')

            return self.df[self.df['peculiar_ceps'].isnull()]
            
        except Exception as e:
            print(e)
            return


    def brasilapi_one(self, parsed_ceps_df:pd.DataFrame, url:str = None, timeout:int = None):
        '''
        Chama a API do BrasilAPI (versão 1) e retorna os dados.\n
        ## Parâmetros\n
        * **parsed_ceps_df:** DataFrame com os CEPs parseados, obtido na função `load_and_parse_forms`.\n
        * **url:** URL da API do BrasilAPI (versão 1).\n
        * **timeout:** Timeout da chamada da API.\n
        ## Retorno\n
        `Dict` contendo:
        * **brasilapi1_df:** DataFrame com os dados da API do BrasilAPI (versão 1).\n
        * **brasilapi1_df_logs:** DataFrame com os logs da API do BrasilAPI (versão 1).
        '''
        self.parsed_ceps_df = parsed_ceps_df
        self.url = self.brasilapi_v1_url
        if timeout != None:
            self.timeout = timeout
        self.timeout = self.default_timesleep

        brasilapi1_df = pd.DataFrame({
            'cep': [],
            'state': [],
            'city': [],
            'neighborhood': [],
            'street': [],
            'service': [],
        })
        brasilapi1_df_logs = pd.DataFrame({
            'ok': [0],
            'nok': [0]
        })
        num_iteration = 0
        with tqdm(total=len(self.parsed_ceps_df)) as pbar:
            for idx, row in self.parsed_ceps_df.iterrows():
                
                clear_output(wait=True)
                replace_cep = row['parsed_ceps']
                get_cep = requests.get(self.url.replace('replace_cep', replace_cep)).json()
        
                if get_cep.get('errors') == None:
                    print(f'num_iteration: {num_iteration}')
                    brasilapi1_df.loc[idx,'cep'] = get_cep.get('cep')
                    brasilapi1_df.loc[idx,'state'] = get_cep.get('state')
                    brasilapi1_df.loc[idx,'city'] = get_cep.get('city')
                    brasilapi1_df.loc[idx,'neighborhood'] = get_cep.get('neighborhood')
                    brasilapi1_df.loc[idx,'street'] = get_cep.get('street')
                    brasilapi1_df.loc[idx,'service'] = get_cep.get('service')
                    brasilapi1_df_logs.loc[0,'ok'] += 1
                else:
                    print(f'num_iteration: {num_iteration}')
                    brasilapi1_df_logs.loc[0,'nok'] += 1
        
                print(tabulate(brasilapi1_df_logs, headers='keys', tablefmt='psql', showindex=False))
                pbar.update()
                sleep(self.timeout)
                num_iteration += 1
        
            return {
                'brasilapi1_df':brasilapi1_df,
                'brasilapi1_df_logs':brasilapi1_df_logs
                }
            

    def brasilapi_two(self, url:str = None, timeout:int = None):
        self.url = self.brasilapi_v2_url
        if timeout != None:
            self.timeout = timeout
        self.timeout = self.default_timesleep

        print(self.url)
        return

    def viacep(self, url:str = None, timeout:int = None):
        self.url = self.viacep_url
        if timeout != None:
            self.timeout = timeout
        self.timeout = self.default_timesleep

        print(self.url)
        return

    def apicep(self, url:str = None, timeout:int = None):
        self.url = self.apicep_url
        if timeout != None:
            self.timeout = timeout
        self.timeout = self.default_timesleep

        print(self.url)
        return

        
# callings = CallsClass()

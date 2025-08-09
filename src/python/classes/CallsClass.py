import os
from time import sleep
import requests
from requests.exceptions import JSONDecodeError, RequestException, Timeout
import pandas as pd
from tqdm import tqdm
from IPython.display import clear_output
from tabulate import tabulate


class CallsClass:
    '''
    Classe que realiza as chamadas aos APIs.\n
    ## Atributos\n
    * **api1:** URL da API 1 (BrasilAPI).\n
    * **api2:** URL da API 2 (ViaCEP).\n
    * **api3:** URL da API 3 (APICEP).\n
    * **path_to_forms:** Caminho para o diretório com os arquivos de formulário.\n
    * **path_to_participants:** Caminho para o diretório com os arquivos de participantes.\n
    ## Métodos\n
    * **load_and_parse_forms:** Carrega e parseia os arquivos de formulário.\n
    * **brasilapi:** Chama da 'Brasil API'.\n
    * **viacep:** Chama a API da 'ViaCEP'.\n
    * **apicep:** Chama a API da 'APICEP'.
    '''

    def __init__(self, default_timesleep:int = 2):
        self.default_timesleep = default_timesleep
        self.brasilapi_url = 'https://brasilapi.com.br/api/cep/v1/replace_cep'
        self.viacep_url = 'https://viacep.com.br/ws/replace_cep/json/'
        self.apicep_url = 'https://cdn.apicep.com/file/apicep/replace_cep.json'
        self.path_to_forms = os.path.join(os.getcwd(), 'src', 'datasets', 'forms')
        self.path_to_participants = os.path.join(os.getcwd(), 'src', 'datasets', 'participants')

    
    def load_and_parse_participants(self):
        '''
        Procura no path dos participantes arquivos excel (.xlsx).\n
        ## Retorno:\n
        DataFrame
        '''
    
        # parsing forms folder files (forms)
        self.forms_files = []
        self.full_path = None
        for root, dirs, files in os.walk(self.path_to_participants):
            self.full_path = root
            for file in files:
                self.forms_files.append(file) if file.endswith('.xlsx') else None

        print('Arquivos encontrados:\n',self.forms_files)
        selected_participants_file = input("Arquivo de participantes para parsear e carregar: ")

        if selected_participants_file not in self.forms_files:
            print("Arquivo de participantes não encontrado.")
            return

        self.full_path = os.path.join(self.full_path, selected_participants_file)
        self.df = pd.read_excel(self.full_path, engine='openpyxl')
        print('Arquivo de participantes carregado.')

        return self.df
    

    def load_and_parse_forms(self):
        '''
        Procura no path dos formulários arquivos excel (.xlsx).\n
        User informa qualquer um deles para tratar e carregar.\n
        Retorna um dataframe com cols do próprio forms com as colunas: 'cep', 'parsed_ceps', 'peculiar_ceps':\n
        * **cep:** CEP do formulário.\n
        * **parsed_ceps:** CEP parseado.\n
        * **peculiar_ceps:** CEPs estranhos (que não foram possíveis de parsear).\n
        ## Das regras:
        * substitui '_' por zero
        * substitui None por ''
        * Se acima de 8 dígitos, pega os 8 primeiros
        * Reinsere hífens no final (XXXXX-XXX)
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
            try:
                ceps_logs = pd.DataFrame({
                    'total de ceps': [f'{total_ceps} (100.00%)'],
                    'ceps com underline': [f'{ceps_with_underline} ({ceps_with_underline/total_ceps:.2%})'],
                    'ceps sem hifen': [f'{ceps_without_hyphen} ({ceps_without_hyphen/total_ceps:.2%})'],
                    'ceps ok': [f'{ceps_ok} ({ceps_ok/total_ceps:.2%})'],
                    'ceps com menos de 8 dígitos': [f'{ceps_too_short} ({ceps_too_short/total_ceps:.2%})'],
                    'ceps com mais de 8 dígitos': [f'{ceps_too_long} ({ceps_too_long/total_ceps:.2%})'],
                    'ceps com tratamento especial': [f'{ceps_peculiar} ({ceps_peculiar/total_ceps:.2%})']
                }).T.rename(columns={0: 'qtde (percent)'})
            except ZeroDivisionError as e:
                print(f'ZeroDivisionError: {e.args[0]}\nChecar arquivo de formulário.')
            except Exception as e:
                print(e.args[0])

            # reinserir o traco removido
            self.df['parsed_ceps'] = self.df['parsed_ceps'].str[0:5] + '-' + self.df['parsed_ceps'].str[5:]

            print(tabulate(ceps_logs, headers='keys', tablefmt='psql'))
            print('Parseado com sucesso.')

            return self.df[self.df['peculiar_ceps'].isnull()]
            
        except Exception as e:
            print(e)
            return


    def brasilapi(self, parsed_ceps_df:pd.DataFrame, url:str = None, timeout:int = None):
        '''
        Chama a API do BrasilAPI e retorna os dados.\n
        ## Parâmetros\n
        * **parsed_ceps_df:** DataFrame com os CEPs parseados, obtido na função `load_and_parse_forms`.\n
        * **url:** URL da API do BrasilAPI.\n
        * **timeout:** time sleep entre cada iteração.\n
        ## Retorno\n
        `Dict` contendo:\n
        * **brasilapi1_df:** DataFrame com os dados da API do BrasilAPI.\n
        * **brasilapi1_df_logs:** DataFrame com os logs da API do BrasilAPI.
        '''
        self.parsed_ceps_df = parsed_ceps_df
        self.url = self.brasilapi_url
        self.timeout = timeout if timeout is not None else self.default_timesleep

        brasilapi_df = pd.DataFrame({'cep': [],'state': [],'city': [],'neighborhood': [],
                                     'street': [],'service': []}, dtype='object')
        
        brasilapi_df_logs = pd.DataFrame({'ok': [0],'nok': [0]}, dtype='object')

        with tqdm(total=len(self.parsed_ceps_df)) as pbar:
            for idx, row in self.parsed_ceps_df.iterrows():
                
                clear_output(wait=True)
                replace_cep = row['parsed_ceps']
                participant_id = str(row['participant_id'])

                try:
                    response = requests.get(self.url.replace('replace_cep', replace_cep))
                    try:
                        get_cep = response.json()
                    except JSONDecodeError as e:
                        print(f'Parsing JSON error: {e.args[0]} | CEP: {replace_cep}')
                        continue
                except (RequestException, Timeout) as e:
                    print(f'response error: {e.args[1]}')
                    continue

                if get_cep.get('errors') == None:
                    brasilapi_df.loc[idx,'item_id'] = participant_id
                    brasilapi_df.loc[idx,'cep'] = get_cep.get('cep')
                    brasilapi_df.loc[idx,'state'] = get_cep.get('state')
                    brasilapi_df.loc[idx,'city'] = get_cep.get('city')
                    brasilapi_df.loc[idx,'neighborhood'] = get_cep.get('neighborhood')
                    brasilapi_df.loc[idx,'street'] = get_cep.get('street')
                    brasilapi_df.loc[idx,'service'] = get_cep.get('service')
                    brasilapi_df_logs.loc[0,'ok'] += 1
                else:
                    brasilapi_df_logs.loc[0,'nok'] += 1
        
                print(tabulate(brasilapi_df_logs, headers='keys', tablefmt='psql', showindex=False))
                pbar.update()
                sleep(self.timeout)

            return {
                'brasilapi_df':brasilapi_df,
                'brasilapi_df_logs':brasilapi_df_logs
                }
    

    def viacep(self, parsed_ceps_df:pd.DataFrame, url:str = None, timeout:int = None):
        '''
        Chama a API do ViaCEP e retorna os dados.\n
        ## Parâmetros\n
        * **parsed_ceps_df:** DataFrame com os CEPs parseados, obtido na função `load_and_parse_forms`.\n
        * **url:** URL da API do ViaCEP.\n
        * **timeout:** time sleep entre cada iteração.\n
        ## Retorno\n
        `Dict` contendo:\n
        * **viacep_df:** DataFrame com os dados da API do ViaCEP.\n
        * **viacep_df_logs:** DataFrame com os logs da API do ViaCEP.
        '''
        self.parsed_ceps_df = parsed_ceps_df
        self.url = url or self.viacep_url
        self.timeout = timeout if timeout is not None else self.default_timesleep

        viacep_df = pd.DataFrame({'item_id':[],'cep': [],'state': [],'city': [],'neighborhood': [],
                                  'street': [],'service': [],}, dtype='object')

        viacep_df_logs = pd.DataFrame({'ok': [0],'nok': [0]}, dtype='object')

        with tqdm(total=len(self.parsed_ceps_df)) as pbar:
            for idx, row in self.parsed_ceps_df.iterrows():
                
                clear_output(wait=True)
                replace_cep = row['parsed_ceps']
                participant_id = str(row['participant_id'])
                
                try:
                    response = requests.get(self.url.replace('replace_cep', replace_cep))
                    try:
                        get_cep = response.json()
                    except JSONDecodeError as e:
                        print(f'json error: {e.args[0]}')
                        continue
                except (RequestException, Timeout) as e:
                    print(f'response error: {e.args[1]}')
                    continue

                if get_cep.get('type') != 'validation_error':
                    viacep_df.loc[idx,'item_id'] = participant_id
                    viacep_df.loc[idx,'item_id'] = row['itemID']
                    viacep_df.loc[idx,'cep'] = get_cep.get('cep')
                    viacep_df.loc[idx,'state'] = get_cep.get('state')
                    viacep_df.loc[idx,'city'] = get_cep.get('city')
                    viacep_df.loc[idx,'neighborhood'] = get_cep.get('neighborhood')
                    viacep_df.loc[idx,'street'] = get_cep.get('street')
                    viacep_df.loc[idx,'service'] = get_cep.get('service')
                    viacep_df_logs.loc[0,'ok'] += 1
                else:
                    viacep_df_logs.loc[0,'nok'] += 1
        
                print(tabulate(viacep_df_logs, headers='keys', tablefmt='psql', showindex=False))
                pbar.update()
                sleep(self.timeout)
            
            return {
                'viacep_df':viacep_df,
                'viacep_df_logs':viacep_df_logs
                }


    def apicep(self, parsed_ceps_df:pd.DataFrame, url:str = None, timeout:int = None):
        '''
        Chama a API do APICEP e retorna os dados.\n
        ## Parâmetros\n
        * **parsed_ceps_df:** DataFrame com os CEPs parseados, obtido na função `load_and_parse_forms`.\n
        * **url:** URL da API do APICEP.\n
        * **timeout:** time sleep entre cada iteração.\n
        ## Retorno\n
        `Dict` contendo:\n
        * **apicep_df:** DataFrame com os dados da API do APICEP.\n
        * **apicep_df_logs:** DataFrame com os logs da API do APICEP.
        '''
        self.parsed_ceps_df = parsed_ceps_df
        self.url = url or self.apicep_url
        self.timeout = timeout if timeout is not None else self.default_timesleep

        apicep_df = pd.DataFrame({'item_id':[],'cep': [],'state': [],'city': [],'neighborhood': [],
                                  'street': [],'service': [],}, dtype='object')

        viacep_df_logs = pd.DataFrame({'ok': [0],'nok': [0]}, dtype='object')

        with tqdm(total=len(self.parsed_ceps_df)) as pbar:
            for idx, row in self.parsed_ceps_df.iterrows():
                
                clear_output(wait=True)
                replace_cep = row['parsed_ceps']
                participant_id = str(row['participant_id'])
                
                try:
                    request = requests.get(self.url.replace('replace_cep', replace_cep))
                    try:
                        get_cep = request.json()
                    except JSONDecodeError as e:
                        print(f'Parsing JSON error: {e.args[0]} | CEP: {replace_cep}')
                        continue
                except (RequestException, Timeout) as e:
                    print(f'response error: {e.args[1]}')
                    continue

                if get_cep.get('code') != 'not_found':
                    apicep_df.loc[idx,'item_id'] = participant_id
                    apicep_df.loc[idx,'cep'] = get_cep.get('cep')
                    apicep_df.loc[idx,'state'] = get_cep.get('state')
                    apicep_df.loc[idx,'city'] = get_cep.get('city')
                    apicep_df.loc[idx,'neighborhood'] = get_cep.get('neighborhood')
                    apicep_df.loc[idx,'street'] = get_cep.get('street')
                    apicep_df.loc[idx,'service'] = get_cep.get('service')
                    viacep_df_logs.loc[0,'ok'] += 1
                else:
                    viacep_df_logs.loc[0,'nok'] += 1
            
                print(tabulate(viacep_df_logs, headers='keys', tablefmt='psql', showindex=False))
                pbar.update()
                sleep(self.timeout)
                
            return {
                'apicep_df':apicep_df,
                'apicep_df_logs':viacep_df_logs
                }
    
    def triforce(
            self,
            parsed_ceps_df:pd.DataFrame,
            bras_url:str = None,
            via_url:str = None,
            cep_url:str = None,
            timeout:int = None
            ):
        '''
        Chama as 3 APIs, com fallbacks.\n
        ## Parâmetros\n
        * **parsed_ceps_df:** DataFrame com os CEPs parseados, obtido na função `load_and_parse_forms`.\n
        * **bras_url:** URL da API do BrasilAPI.\n
        * **via_url:** URL da API do Viacep.\n
        * **cep_url:** URL da API do APICEP.\n
        * **timeout:** time sleep entre cada iteração.\n
        ## Retorno\n
        `Dict` contendo:\n
        * **complete_api_df:** DataFrame com os dados obtidos das APIs.\n
        * **complete_api_df_logs:** DataFrame com os logs das APIs.
        '''
        self.parsed_ceps_df = parsed_ceps_df
        self.bras_url = bras_url or self.brasilapi_url
        self.via_url = via_url or self.viacep_url
        self.cep_url = cep_url or self.apicep_url
        self.timeout = timeout if timeout is not None else self.default_timesleep
        
        complete_api_df = pd.DataFrame({'item_id':[],'cep': [],'state': [],'city': [],'neighborhood': [],
                                  'street': [],'service': [],}, dtype='object')
        complete_api_df_logs = pd.DataFrame({'ok': [0],'nok': [0]}, dtype='object')

        ceps_errors_df = pd.DataFrame({
            'brasil api errors': [0],'viacep errors': [0],'apicep errors': [0]
            }, dtype='int')

        with tqdm(total=len(self.parsed_ceps_df)) as pbar:
            for idx, row in self.parsed_ceps_df.iterrows():

                clear_output(wait=True)
                replace_cep = row['parsed_ceps']
                participant_id = str(row['participant_id'])

                try:
                    response = requests.get(self.bras_url.replace('replace_cep', replace_cep)).json()
                    response['cep']
                except:
                    clear_output(wait=True)
                    ceps_errors_df.loc[0,'brasil api errors'] += 1
                    print(tabulate(complete_api_df_logs, headers='keys', tablefmt='psql', showindex=False))
                    print(tabulate(ceps_errors_df, headers='keys', tablefmt='psql', showindex=False))
                    try:
                        response = requests.get(self.via_url.replace('replace_cep', replace_cep)).json()
                        response['cep']
                    except:
                        clear_output(wait=True)
                        ceps_errors_df.loc[0, 'viacep errors'] += 1
                        print(tabulate(complete_api_df_logs, headers='keys', tablefmt='psql', showindex=False))
                        print(tabulate(ceps_errors_df, headers='keys', tablefmt='psql', showindex=False))
                        try:
                            response = requests.get(self.cep_url.replace('replace_cep', replace_cep)).json()
                            response['cep']
                        except:
                            clear_output(wait=True)
                            ceps_errors_df.loc[0, 'apicep errors'] += 1
                            complete_api_df_logs.loc[0,'nok'] += 1
                            print(tabulate(complete_api_df_logs, headers='keys', tablefmt='psql', showindex=False))
                            print(tabulate(ceps_errors_df, headers='keys', tablefmt='psql', showindex=False))
                            pbar.update()
                            sleep(self.timeout)
                            continue

                complete_api_df.loc[idx,'item_id'] = participant_id
                complete_api_df.loc[idx,'cep'] = response.get('cep')
                complete_api_df.loc[idx,'state'] = response.get('state')
                complete_api_df.loc[idx,'city'] = response.get('city')
                complete_api_df.loc[idx,'neighborhood'] = response.get('neighborhood')
                complete_api_df.loc[idx,'street'] = response.get('street')
                complete_api_df.loc[idx,'service'] = response.get('service')
                complete_api_df_logs.loc[0,'ok'] += 1
                
                print('ceps logs')
                print(tabulate(complete_api_df_logs, headers='keys', tablefmt='psql', showindex=False))
                print(tabulate(ceps_errors_df, headers='keys', tablefmt='psql', showindex=False))
                pbar.update()
                sleep(self.timeout)

            return {
                'complete_api_df':complete_api_df,
                'complete_api_df_logs':complete_api_df_logs,
                'ceps_errors_df':ceps_errors_df
            }

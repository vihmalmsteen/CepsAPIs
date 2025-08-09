import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint
from sqlalchemy.exc import ProgrammingError as SQLAlchemyProgrammingError, OperationalError as SQLAlchemyOperationalError
from pymysql.err import ProgrammingError as PyMySQLProgrammingError, OperationalError as PyMySQLOperationalError


class DBData:
    '''
    Inicia conn com DB. Depende de config do '.env'.\n
    ## Métodos:\n
    * **query_data:** Abre uma conexão com o DB MySQL usando as configs do `.env`, consulta e exporta os dados.\n
    * **editions:** Retorna as edições disponiveis em `eventcomplement.globalEvent`.
    '''
    
    def __init__(
            self, 
            host=os.getenv('HOST'), 
            user=os.getenv('USER'), 
            password=os.getenv('PASSWORD'), 
            database=os.getenv('DATABASE')
            ):
        load_dotenv()
        self.host = host or os.getenv('HOST')
        self.user = user or os.getenv('USER')
        self.password = password or os.getenv('PASSWORD')
        self.database = database or os.getenv('DATABASE')
        self.endpoint = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}?charset=utf8mb4"
    

    def query_data(
            self,
            edicao:str='Teste',
            data_compra_ini:str='1900-01-01',
            data_compra_fini:str='2100-12-31',
            limit_max_rows:int=10000000,
            total_rows_in_batches:int=10000000
            ):
        '''
        Abre uma conexão com o DB MySQL usando as configs do `.env`.\n
        Consulta o DB com a query em `select_ceps.sql`.\n
        A query recebe os args do método.\n
        ## Args \n
        * **total_rows_in_batches (int, optional):** Nº de registros nos batches pra separar o dataset.\n
        * **edicao (str, optional):** Edição do evento, `eventcomplement.globalEvent`.\n
        * **data_compra_ini (str, optional):** Data de compra inicial.\n
        * **data_compra_fini (str, optional):** Data de compra final.\n
        * **limit_max_rows (int, optional):** Limite de linhas.\n
        ## Retorno:\n
        * **DataFrame:** DataFrame com os dados da query.**
        '''

        self.connection = create_engine(self.endpoint).connect()
        self.total_rows_in_batches = total_rows_in_batches
        self.edicao = edicao
        self.data_compra_ini = data_compra_ini
        self.data_compra_fini = data_compra_fini
        self.limit_max_rows = limit_max_rows
        
        try:
            with self.connection as conn:
                
                # paths das queries
                forms_ceps_txt_path = os.path.join(os.getcwd(), 'src', 'queries', 'select_ceps.sql')
                participants_txt_path = os.path.join(os.getcwd(), 'src', 'queries', 'select_participants.sql')
                
                
                # select e export dos forms
                print('\nconsulting ceps...')
                with open(forms_ceps_txt_path, "r", encoding='utf-8') as f:
                    mappings_forms = {
                        'total_rows_in_batches': self.total_rows_in_batches,
                        'edicao': self.edicao,
                        'data_compra_ini': self.data_compra_ini,
                        'data_compra_fini': self.data_compra_fini,
                        'limit_max_rows': self.limit_max_rows
                        }
                
                    query = f.read().format_map(mappings_forms)
                    forms_df = pd.read_sql(query, conn)
                    forms_df.to_excel(
                        os.path.join(
                            os.getcwd(),
                            'src','datasets','forms',
                            f'forms_results_{self.edicao}.xlsx'.replace(' ', '_')
                            ),
                        index=False, engine='openpyxl'
                    )
                    
                    if len(forms_df) == 0:
                        print(f'Nenhum cep encontrado. Checar args e query. Mappings:')
                        pprint(mappings_forms)
                        return 
                    
                    print(f'Total de registros carregados e exportados: {len(forms_df)} ceps.')
                

                # select e export dos participants
                print('\nconsulting participants...')
                with open(participants_txt_path, "r", encoding='utf-8') as f:
                    mappings_participants = {
                        'total_rows_in_batches': self.total_rows_in_batches,
                        'edicao': self.edicao,
                        'data_compra_ini': self.data_compra_ini,
                        'data_compra_fini': self.data_compra_fini,
                        'limit_max_rows': self.limit_max_rows
                        }

                    query = f.read().format_map(mappings_participants)
                    participants_df = pd.read_sql(query, conn)
                    participants_df.to_excel(
                        os.path.join(
                            os.getcwd(),
                            'src','datasets','participants',
                            f'participants_results_{self.edicao}.xlsx'.replace(' ', '_')
                            ),
                        index=False, engine='openpyxl'
                    )
                    
                    if len(participants_df) == 0:
                        print(f'Nenhum participante encontrado. Checar args e query. Mappings:')
                        pprint(mappings_participants)
                        return 
                    
                    print(f'Total de registros carregados e exportados: {len(participants_df)} participants.')
                    return participants_df
                

        except (SQLAlchemyProgrammingError, PyMySQLProgrammingError) as err:
            return print(f'Erro de conn/objs do DB inexistentes: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except (SQLAlchemyOperationalError, PyMySQLOperationalError) as err:
            return print(f'Erro de sintaxe de query: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except Exception as err:
            return print(f'Erro geral: \n args: {err.args}')
        
        
    def editions(self, like_param:str=None):
        '''
        ## Args \n
        * **like_param (str, optional):** Parametro para filtrar as edicoes. Default -> pega todas edições.
        ## Retorno:\n
        * **DataFrame:** DataFrame com as edicoes disponíveis em `eventcomplement.globalEvent`.
        '''
        
        self.connection = create_engine(self.endpoint).connect()
        self.like_param = like_param

        filter_append_parsed = None
        
        if self.like_param:
            filter_append_parsed = f'where lower(globalEvent) like lower("%%{self.like_param}%%")'
        else:
            filter_append_parsed = ''
        
        try:
            with self.connection as conn:
                editions_txt_path = os.path.join(os.getcwd(), 'src', 'queries', 'select_editions.sql')
                
                with open(editions_txt_path, "r") as f:
                    mappings = {
                        'filter_append': filter_append_parsed
                        }
                    
                    query = f.read().format_map(mappings)
                    df = pd.read_sql(query, conn)

                    # print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

                    return df
                
        except (SQLAlchemyProgrammingError, PyMySQLProgrammingError) as err:
            return print(f'Erro de conn/objs do DB inexistentes: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except (SQLAlchemyOperationalError, PyMySQLOperationalError) as err:
            return print(f'Erro de sintaxe de query: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except Exception as err:
            return print(f'Erro geral: \n args: {err.args}')


    def ScrapDB(
            self,
            query_or_list_editions:int = 2,
            edicao:str = None,
            data_compra_ini:str = None,
            data_compra_fini:str = None,
            total_rows_in_batches:int = None,
            limit_max_rows:int = None):
        '''
        Duas opções:\n
        * **Consultar formulários e participantes:** exporta para excel ambos arquivos na pasta datasets.\n
        * **Consultar edições:** retorna um DataFrame com as edições disponíveis em `eventcomplement.globalEvent`.\n
        ## Args \n
        * **query_or_list_editions (int, optional):** 1 para consultar formulários e participantes, 2 para consultar edições. Default -> 2.\n
        * **edicao (str, optional):** Edição do evento, `eventcomplement.globalEvent`.\n
        * **data_compra_ini (str, optional):** Data de compra inicial.\n
        * **data_compra_fini (str, optional):** Data de compra final.\n
        * **total_rows_in_batches (int, optional):** Nº de registros nos batches pra separar o dataset.\n
        * **limit_max_rows (int, optional):** Limite de registros no dataset.\n
        '''
        self.query_or_list_editions = query_or_list_editions
        self.edicao = edicao or DBData.query_data.__defaults__[0]
        self.data_compra_ini = data_compra_ini or DBData.query_data.__defaults__[1]
        self.data_compra_fini = data_compra_fini or DBData.query_data.__defaults__[2]
        self.total_rows_in_batches = total_rows_in_batches or DBData.query_data.__defaults__[3]
        self.limit_max_rows = limit_max_rows or DBData.query_data.__defaults__[4]

        if int(self.query_or_list_editions) == 1:
            self.queried_data = self.query_data(
                edicao =                DBData.query_data.__defaults__[0] if edicao == ''                else self.edicao, 
                data_compra_ini =       DBData.query_data.__defaults__[1] if data_compra_ini == ''       else self.data_compra_ini, 
                data_compra_fini =      DBData.query_data.__defaults__[2] if data_compra_fini == ''      else self.data_compra_fini, 
                limit_max_rows =        DBData.query_data.__defaults__[3] if limit_max_rows == ''        else self.limit_max_rows, 
                total_rows_in_batches = DBData.query_data.__defaults__[4] if total_rows_in_batches == '' else self.total_rows_in_batches
                )
            print('Forms e participants carregados e exportados.')
            return
        
        elif int(self.query_or_list_editions) == 2:
            self.queried_editions = self.editions(
                like_param = self.edicao
                )
            print('Edições carregadas.')
            return self.queried_editions
        
        else:
            return

import os
import dotenv
from sqlalchemy import create_engine
import pandas as pd
from tabulate import tabulate
from pprint import pprint
from tqdm import tqdm
from IPython.display import clear_output
from sqlalchemy.exc import ProgrammingError as SQLAlchemyProgrammingError, OperationalError as SQLAlchemyOperationalError
from pymysql.err import ProgrammingError as PyMySQLProgrammingError, OperationalError as PyMySQLOperationalError


dotenv.load_dotenv()


class DBData:
    '''
    Inicia conn com DB. Depende de config do '.env'.\n
    ## methods\n
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
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.endpoint = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}?charset=utf8mb4"
    

    def query_data(
            self,
            edicao='Teste',
            data_compra_ini='1900-01-01',
            data_compra_fini='2100-12-31',
            limit_max_rows=10000000,
            total_rows_in_batches=10000000
            ):
        '''
        Abre uma conexão com o DB MySQL usando as configs do `.env`.\n
        Consulta o DB com a query em `select_ceps.sql` e exporta os dados em um arquivo JSON.\n
        A query recebe os args do método.\n
        ## Args \n
        * **total_rows_in_batches (int, optional):** Nº de registros nos batches pra separar o dataset.\n
        * **edicao (str, optional):** Edição do evento, `eventcomplement.globalEvent`.\n
        * **data_compra_ini (str, optional):** Data de compra inicial.\n
        * **data_compra_fini (str, optional):** Data de compra final.\n
        * **limit_max_rows (int, optional):** Limite de linhas.
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
                        os.path.join(os.getcwd(), 'src', 'datasets', 'forms', f'forms_results_{self.edicao}.xlsx'.replace(' ', '_')),
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
                        os.path.join(os.getcwd(), 'src', 'datasets', 'participants', f'participants_results_{self.edicao}.xlsx'.replace(' ', '_')),
                        index=False, engine='openpyxl'
                    )
                    
                    if len(participants_df) == 0:
                        print(f'Nenhum participante encontrado. Checar args e query. Mappings:')
                        pprint(mappings_participants)
                        return 
                    
                    print(f'Total de registros carregados e exportados: {len(participants_df)} participants.')
                    return
                

        except (SQLAlchemyProgrammingError, PyMySQLProgrammingError) as err:
            return print(f'Erro de conn/objs do DB inexistentes: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except (SQLAlchemyOperationalError, PyMySQLOperationalError) as err:
            return print(f'Erro de sintaxe de query: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except Exception as err:
            return print(f'Erro geral: \n args: {err.args}')
        
        
    def editions(self, like_param=None):
        '''
        Retorna as edicoes disponiveis em `eventcomplement.globalEvent`.\n
        ## Args \n
        * **like_param (str, optional):** Parametro para filtrar as edicoes. Default -> pega todas edições.
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

                    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

                    return
                
        except (SQLAlchemyProgrammingError, PyMySQLProgrammingError) as err:
            return print(f'Erro de conn/objs do DB inexistentes: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except (SQLAlchemyOperationalError, PyMySQLOperationalError) as err:
            return print(f'Erro de sintaxe de query: \n args: {err.args} \n SQLALCHEMY_CODE_ERROR: {err.code}')
        except Exception as err:
            return print(f'Erro geral: \n args: {err.args}')

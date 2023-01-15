"""
Autor: Victor Gimenes
Data: 07/12/2022

Projeto:
- Módulo criado para armazenar as funções responsáveis pela execução de queries dentro do banco de dados da Tenax

"""
# Importando bibliotecas externas necessárias
import importlib
import pandas as pd
import numpy as np
import sqlalchemy as sa
# Importando o módulo local connect_tenax_db
from .  import connect_tenax_db as db
# Para permitir que mudanças passem a ser usadas diretamente
importlib.reload(db)


#! BLOCO 1 (GENÉRICOS): : Inser genérico no banco de dados da Tenax
def params_generic_query(df) -> str:
    """
    - Função responsável por auxiliar a parametrização das queries de
      insert para qualquer dataframe.

    obs.: é utilizada na função insert_tbl deste módulo.

    - Args:
        df (pd.Dataframe): dataframe de interesse.

    - Returns:
        str: devolve as lacunas necessárias para a parametrização do insert.
    """         
    columns_list = df.columns.tolist()
    columns = f'({(",".join(columns_list))})'
    for i in range(len(columns_list)): columns_list[i]='?'
    params = f'({(",".join(columns_list))})' 
    return columns, params  

def insert_tbl(MY_TABLE,df):    # sourcery skip: extract-method
    """
    - Função genérica responsável pela:
        (i)  Conexão com o banco de dados da Tenax.
        (ii) Insert parametrizado do dataframe df na tabela de interesse dentro do
             banco de dados.
    
    Obs.: a função utiliza o método fast_executemany.
    
    - Args:
        MY_TABLE (str): nome da tabela que deverá ser atualizada com os dados.
        df (pd.Dataframe): dataframe que irá ser inserido na tabela MY_TABLE.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Iniciando conexão
        conn = db.get_connection()
        # Criando cursor
        cursor = conn.cursor()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados Tenax: {e}.' from e   
    # Parametrizando a query de insert com o auxílio da função params_generic_query
    params = params_generic_query(df)
    COLUMNS = params[0]
    PARAMS = params[1]
    insert = f"INSERT INTO {MY_TABLE}{COLUMNS} VALUES {PARAMS}"
    # Setando que usaremos o método fast
    cursor.fast_executemany = True
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Executando a query acima
    try:
        cursor.executemany(insert, df.values.tolist())
        cursor.commit()
    except Exception as e:
        # Caso algum erro seja gerado a partir da query, forçaremos um erro para parar a execução
        # junto ao método rollback para preserveção da tabela
        cursor.rollback()
        raise f'Erro na execução do insert na tbl {MY_TABLE} no banco de dados da Tenax: {e}.' from e
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        cursor.close()
        conn.close()
    
def upsert_tbl(MY_TABLE:str, keys:list, df:pd.DataFrame):
    """
    - Função genérica responsável pela:
        (i)  Conexão com o banco de dados da Tenax.
        (ii) Upsert parametrizado do dataframe df na tabela de interesse dentro do
             banco de dados.

    - Args:
        MY_TABLE (str): nome da tabela que deverá ser atualizada com os dados.
        keys (list): lista contendo as colunas que deverão ser consideradas como keys.
        df (pd.DataFrame): dataframe que irá ser inserido na tabela MY_TABLE.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Iniciando conexão
        conn = db.get_connection()
        # Criando cursor
        cursor = conn.cursor()    
    except Exception as e:
        print(f'Erro na conexão com o banco de dados da Tenax: {e}.')

    # Setando que usaremos o método fast
    cursor.fast_executemany = True
    # Preparando oo parâmetros para a parametrização do upsert 
    #! Listando as colunas da base separadas por vírgulas (prepara a listagem das colunas da tbl)
    df_columns = f'({(",".join(df.columns.tolist()))})' # Exemplo: COL_A,COL_B,COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "Source." e separados por vírgulas  (prepara as colunas para o match do upsert)
    source_columns_list = [f'Source.{i}' for i in df.columns.tolist()]
    source_columns_params = f'({(",".join(source_columns_list))})' # Exemplo: Source.COL_A,Source.COL_B,Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "=Source." e separados por vírgulas (prepara as colunas para o update)
    update_columns_list = [f'{i}=Source.{i}' for i in df.columns.tolist()]
    update_columns_params = f'{",".join(update_columns_list)}'  # Exemplo: =Source.COL_A,=Source.COL_B,=Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "Target.", concatenado com "=". "Source." e separados por " and " (prepara o match essencial para o upsert)
    match_columns_list = [f'Target.{i}=Source.{i}' for i in keys]
    match_columns_params = f'{" and ".join(match_columns_list)}'  # Exemplo: =Source.COL_A,=Source.COL_B,=Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Lisando uma string com os espaços representados por "?" a depender da quantidade de colunas da tbl (prepara os parâmetros para a inserção das novas observações do insert e update)
    columns_params = ['?' for _ in range(len(df.columns.tolist()))]
    columns_params = str(columns_params).replace("[","").replace("]","").replace("'","") # Exemplo: ?,?,? .... a depender da quantidade de colunas da tabela
    # for i in range(len(df.columns.tolist())): df.columns.tolist()[i]='?'
    # columns_params = str(df.columns.tolist()).replace("[","").replace("]","").replace("'","") # Exemplo: ?,?,? .... a depender da quantidade de colunas da tabela
    # Criando a query de upsert a partir dos parâmetros anteriores
    sql = f"MERGE INTO {MY_TABLE} as Target \
            USING (SELECT * FROM (VALUES ({columns_params})) AS s {df_columns}) AS Source \
                ON {match_columns_params} \
            WHEN NOT MATCHED THEN \
                INSERT {df_columns} VALUES {source_columns_params} \
            WHEN MATCHED THEN \
                UPDATE SET {update_columns_params};"
    # Excutando a query
    try:
        cursor.executemany(sql, df.values.tolist())
        cursor.commit()
    except Exception as e:
        # Caso algum erro seja gerado a partir da query, forçaremos um erro para parar a execução
        # junto ao método rollback para preserveção da tabela
        cursor.rollback()
        print(f'Erro na execução do upsert na tbl {MY_TABLE} do banco de dados da Tenax: {e}.') 
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        cursor.close()
        conn.close()

def upsert_tbl_sa(MY_TABLE:str, keys:list, df:pd.DataFrame):
    """
    - Função genérica responsável pela:
        (i)  Conexão com o banco de dados da Tenax.
        (ii) Upsert parametrizado do dataframe df na tabela de interesse dentro do
             banco de dados.

    - Args:
        MY_TABLE (str): nome da tabela que deverá ser atualizada com os dados.
        keys (list): lista contendo as colunas que deverão ser consideradas como keys.
        df (pd.DataFrame): dataframe que irá ser inserido na tabela MY_TABLE.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Iniciando conexão
        conn = db.get_connection_sa()
    except Exception as e:
        print(f'Erro na conexão com o banco de dados da Tenax: {e}.')
        return 
    # Setando que usaremos o método fast

    # Preparando oo parâmetros para a parametrização do upsert 
    #! Listando as colunas da base separadas por vírgulas (prepara a listagem das colunas da tbl)
    df_columns = f'({(",".join(df.columns.tolist()))})' # Exemplo: COL_A,COL_B,COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "Source." e separados por vírgulas  (prepara as colunas para o match do upsert)
    source_columns_list = [f'Source.{i}' for i in df.columns.tolist()]
    source_columns_params = f'({(",".join(source_columns_list))})' # Exemplo: Source.COL_A,Source.COL_B,Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "=Source." e separados por vírgulas (prepara as colunas para o update)
    update_columns_list = [f'{i}=Source.{i}' for i in df.columns.tolist()]
    update_columns_params = f'{",".join(update_columns_list)}'  # Exemplo: =Source.COL_A,=Source.COL_B,=Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Listando as colunas da base de dados com o prefixo "Target.", concatenado com "=". "Source." e separados por " and " (prepara o match essencial para o upsert)
    match_columns_list = [f'Target.{i}=Source.{i}' for i in keys]
    match_columns_params = f'{" and ".join(match_columns_list)}'  # Exemplo: =Source.COL_A,=Source.COL_B,=Source.COL_C ... a depender da quantidade de colunas da tabela
    #! Lisando uma string com os espaços representados por "?" a depender da quantidade de colunas da tbl (prepara os parâmetros para a inserção das novas observações do insert e update)
    columns_params = []
    for i in range(len(df.columns.tolist())): 
        columns_params.append(f'?{i}')
    columns_params = str(columns_params).replace("[","").replace("]","").replace("'","") # Exemplo: ?,?,? .... a depender da quantidade de colunas da tabela
    # Criando a query de upsert a partir dos parâmetros anteriores
    sql = f"MERGE INTO {MY_TABLE} as Target \
            USING (SELECT * FROM (VALUES ({columns_params})) AS s {df_columns}) AS Source \
                ON {match_columns_params} \
            WHEN NOT MATCHED BY Target THEN \
                INSERT {df_columns} VALUES {source_columns_params} \
            WHEN MATCHED THEN \
                UPDATE SET {update_columns_params};"
    # Excutando a query
    try:
        for params_list in df.values.tolist():
            sql_line = sql
            for i, param in enumerate(params_list):
                if type(param) is str:
                    param = "'"+param+"'"
                sql_line = sql_line.replace(f'?{i}', str(param))
            with conn.begin():
                conn.execute(sql_line)

    except Exception as e:
        # Caso algum erro seja gerado a partir da query, forçaremos um erro para parar a execução
        # junto ao método rollback para preserveção da tabela
        raise f'Erro na execução do upsert na tbl {MY_TABLE} do banco de dados da Tenax: {e}.' from e
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        conn.close()    


def read_tbl(MY_TABLE: str) -> pd.DataFrame:    # sourcery skip: extract-method
    """
    Função genérica responsável pela:
        (i) Conexão com o banco de dados da Tenax.
        (ii) Select da tabela MY_TABLE do banco de dados.

    Args:
        MY_TABLE (str): nome da tabela de interesse no banco de dados.
        
    Returns:
        pd.DataFrame: dados resultantes da query.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Iniciando conexão via sqlalchemy
        conn = db.get_connection_sa()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados Tenax: {e}.' from e
    # Criando query a partir da tbl de input
    sql = f"SELECT * FROM {MY_TABLE}"
    # Executando a query acima
    try:
        # realizando select via sqlalchemy
        df = pd.read_sql_query(sql,con=conn,index_col=None)
        return df
    except Exception as e:
        # Caso algum erro seja gerado a partir da query, forçaremos um erro para parar a execução
        raise f'Erro na execução da query da tbl {MY_TABLE} no banco de dados da Tenax: {e}.' from e
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        conn.close()

def read_tbl_custom(sql: str) -> pd.DataFrame:    # sourcery skip: extract-method
    """
    Função responsável pela:
        (i) Conexão com o banco de dados da Tenax.
        (ii) Select da tabela MY_TABLE do banco de dados.

    Args:
        sql (str): select customizada para consumo de dados de tbl do banco.
        
    Returns:
        pd.DataFrame: dados resultantes da query informada no input.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Iniciando conexão via sqlalchemy
        conn = db.get_connection_sa()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados da Tenax: {e}.' from e
    # Executando a query de input
    try:
        # realizando select via sqlalchemy
        df = pd.read_sql_query(sql,con=conn,index_col=None)
        return df
    except Exception as e:
        # Caso algum erro seja gerado a partir da query, forçaremos um erro para parar a execução
       raise f'Erro na execução da query no banco de dados da Tenax: {e}.' from e
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        conn.close()

def execute_custom_query(sql:str):
    """
    Função genérica responsável pela:
        (i)  Conexão com o banco de dados da Tenax.
        (ii) Execução da query passada como input da função (sql).

    obs: deve ser utilizada somente para gestão de tabelas, já temos 
         outras funções para selects genéricos e customizados nesse módulo.
    
    Args:
        sql (str): query a ser executada dentro da função - foco na gestão de tbl's.
    """
    # Criando a conexão com o banco de dados a partir do módulo db
    try:
        # Inician conexão
        conn = db.get_connection()
        # Criando cursor
        cursor = conn.cursor()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados Tenax: {e}.' from e
    # Executando a query de input 
    try:
        cursor.execute(sql)
        cursor.commit()
    except Exception as e:
        # Caso algum erro seja gerado a partir da query, iremos refazer as alterações
        cursor.rollback()
        raise f'Erro na execução da query no banco de dados da Tenax: {e}.' from e
    finally:
        # Em qualquer cenário, temos que fechar a conexão com a base de dados.
        conn.close()


def execute_custom_query_sa(sql:str):
    try:
        conn = db.get_connection_sa()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados Tenax: {e}.' from e
    try:
        with conn.begin():
            conn.execute(sql)
    finally:
        conn.close()


def insert_tbl_sa(MY_TABLE,df):    # sourcery skip: extract-method
    try:
        conn = db.get_connection_sa()
    except Exception as e:
        raise f'Erro na conexão com o banco de dados Tenax: {e}.' from e   

    params = params_generic_query(df)
    COLUMNS = params[0]
    PARAMS = params[1]
    insert = f"INSERT INTO {MY_TABLE}{COLUMNS} VALUES {PARAMS}"
    test_list = []
    for params_list in df.values.tolist():
        sql_line = insert
        for param in params_list:
            new_params = param.split(';')
            for i, value in enumerate(new_params):
                try:
                    float(value)
                except:
                    value = "'{}'".format(value)
                new_params[i] = value
            param = ', '.join(new_params)
            sql_line = sql_line.replace('?', param).replace(';', ', ')
            with conn.begin():
                conn.execute(sql_line)
    conn.close()

        
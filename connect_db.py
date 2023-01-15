"""
Author: Victor Gimenes
Date: 07/12/2022
Módulo criado para armazenar as funções de conexão com bancoS de dados em Azure.
"""

# Importando os módulos que geram a conexão com o banco
import sqlalchemy as sa # Primeira possibilidade: Sqlalchemy
import pyodbc # Segunda possibilidade: Pyodbc

def get_user():
    # Alterar o usuário aqui
    return "insert the username here!"

def get_pass():
    # Alterar a senha aqui
    return "insert the password here!"

def get_driver()
    # Alterar o driver aqui
    return "insert the driver name here!"

def get_server()
    # Alterar o server aqui
    return "insert the server name here!"

def get_database()
    # Alterar o server aqui
    return "insert the database name here!"

def get_string_connection():
    """String de conexão via pyodbc"""
    driver = get_driver()
    server = get_server()
    database = get_database()
    username = get_user()
    password = get_pass()
    return "Driver={0};Server={1};Database={2};UID={3};PWD={4};".format(driver, server, database, username, password)

def get_string_connection_sa():
    """String de conexão via sqlalchemy"""
    username = get_user()
    password = get_pass()
    server = get_server()
    database = get_database()
    return 'mssql+pyodbc://{0}:{1}@{2}/{3}?driver=SQL+Server'.format(username, password.replace('@', '%40'), server, database)
      
def get_connection():
    """
    Função que retorna a conexão com o banco de dados via pyodbc. 
    
    É necessário instalar o driver ODBC Driver 18 for SQL Server no Link:
    https://docs.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
    
    Obs.: sempre que usar a função, lembrar de fechar a conexão com o banco.
    
    Returns:
        obj: conexão com o banco de dados.
    """
    connection = pyodbc.connect(get_string_connection())
    return connection

def get_connection_sa():
    """
    Função que retorna a conexão com o banco de dados via sqlalchemy (melhor para o uso via pandas). 
    
    Obs.: sempre que usar a função, lembrar de fechar a conexão com o banco.
    
    Returns:
        obj: conexão com o banco de dados.
    """
    engine = sa.create_engine(get_string_connection_sa())
    connection = engine.connect()
    return connection
    

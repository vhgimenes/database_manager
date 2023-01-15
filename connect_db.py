"""
Author: Victor Gimenes
Date: 07/12/2022

Módulo criado para armazenar as funções de conexão com bancoS de dados em Azure.
"""
# Importando os módulos que geram a conexão com o banco
# Primeira possibilidade: Sqlalchemy
import sqlalchemy as sa
# Segunda possibilidade: Pyodbc
import pyodbc

def get_user():
    # Alterar o usuário aqui
    return "insert the username here!"

def get_pass():
    # Alterar a senha aqui
    return "insert the password here!"

def get_string_connection():
    # Alterar o driver aqui
    driver = "insert the driver name here!"
    # Alterar o server aqui
    server = "insert the server name here!"
    # Alterar o database aqui
    database = 'insert the database name here!'
    username = get_user()
    password = get_pass()
    return "Driver={0};Server={1};Database={2};UID={3};PWD={4};PORT=1433;".format(driver, server, database, username, password)

def get_string_connection_sa():
    return 'mssql+pyodbc://{0}:{1}@tenax.database.windows.net/tenax-db?driver=SQL+Server'.format(get_user(), get_pass().replace('@', '%40'))
      
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
    

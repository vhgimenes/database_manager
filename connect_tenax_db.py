"""
Autor: Victor Gimenes
Data: 07/12/2022

Projeto:
- Módulo criado com as funçõe que garantem a conexão com o banco de dados da Tenax Capital (SQL Server).
"""
#Importando os módulos que geram a conexão com o banco
# Primeira possibilidade: Sqlalchemy
import sqlalchemy as sa
# Segunda possibilidade: Pyodbc
import pyodbc

def get_user():
    #alterar o usuarios aqui
    return "tenax-sa"

def get_pass():
    #alterar a senha aqui
    return "tp§xM4:2'sRJgQv]MxZn-mk-+y{_)9"

def get_string_connection():
    #alterar o driver aqui
    driver = "{ODBC Driver 18 for SQL Server}"
    #alterar o server aqui
    server = "tcp:tenax.database.windows.net"
    #alterar o database aqui
    database = 'tenax-db'
    username = get_user()
    password = get_pass()
    return "Driver={0};Server={1};Database={2};UID={3};PWD={4};PORT=1433;".format(driver, server, database, username, password)

def get_string_connection_sa():
    return 'mssql+pyodbc://{0}:{1}@tenax.database.windows.net/tenax-db?driver=SQL+Server'.format(get_user(), get_pass().replace('@', '%40'))
      
def get_connection():
    """
    Função que retorna a conexão com o banco de dados da Tenax. 
    
    Para se poder utilizar a função, é necessário instalar o driver ODBC Driver 18 for SQL Server (x64, não necessariamente) no Link:
    https://docs.microsoft.com/pt-br/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
    
    Obs.: sempre que usar a função, lembrar de fechar a conexão com o banco.
    
    Returns:
        obj: conexão com o banco de dados da Tenax Capital.
    """
    connection = pyodbc.connect(get_string_connection())
    return connection

def get_connection_sa():
    """
    Função que retorna a conexão com o banco de dados da Tenax via sqlalchemy. 
    
    Obs.: sempre que usar a função, lembrar de fechar a conexão com o banco.
    
    Returns:
        obj: conexão com o banco de dados da Tenax Capital.
    """
    engine = sa.create_engine(get_string_connection_sa())
    connection = engine.connect()
    return connection
    
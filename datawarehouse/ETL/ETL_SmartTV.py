# =====================================
#  ETL: DataBase "SMARTS"
#  Autoras: Laura Chaves, Fiorella Righelato, Daiana Gareis
#  GitHub: https://github.com/Laura-Chaves/Datawarehouse_SmartsTV
# =====================================



# ===========
#  Bibliotecas
# ===========

import pyodbc # Conexión con la base de datos
import configparser # Configuration of the database
from sqlalchemy import create_engine # Creation of the connection to the DB

import pandas as pd # Handling of dataframes
import numpy as np # Handling of arrays
import re # Regular expressions

from modules.update_dimensions_table import updateDimensionTable, updateDimensionTableIntPK # Function to update dimensions tables



# =================================
#  Connection with the Original DB
#  
#  Requirements:
#  - ODBC Driver: https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
#  - Microsoft Access Driver: https://www.microsoft.com/en-us/download/details.aspx?id=13255
# =================================

# Name of the ODBC DSN
dsn_name = 'base_ElProfesional'

# Read the configuration from the 'secrets.ini' file (contains username and password)
config = configparser.ConfigParser()
config.read('./datawarehouse/ETL/secrets.ini')
username = config['database']['username']
password = config['database']['password']

conn = pyodbc.connect(f'DSN={dsn_name};UID={username};PWD={password}')
cursor = conn.cursor()


# ===================================
#  Connection with the DataWarehouse
# ===================================
# Read the configuration from the 'secrets.ini' file (contains username and password)
DW_username = config['datawarehouse']['DW_username']
DW_password = config['datawarehouse']['DW_password']

# Create the connection engine
engine_cubo = create_engine(f"postgresql://{DW_username}:{DW_password}@localhost/ElProfesional_DW")



# =======================================
#  Save Information from the Original DB
# =======================================

DB_tablesNamesToConsult = ['Articulos', 'CabVentas', 'Clientes', 'ItemVentas', 'Rubros', 'TipoCliente', 'Vendedor']

# Store DB Tables
table_info_list = [] # List of dictionaries to store information about the tables
DB_tables = {}  # Stores the dataframes of the tables that contain data


for table in DB_tablesNamesToConsult:
    query = f'SELECT * FROM {table}'
    # Store the result in a dataframe
    DB_tables[table] = pd.read_sql(query, conn)

conn.close()



# ================================================
#  Data loading, cleaning, and dimension creation
# ================================================

# Create a dataframe with the data from the 'CabVentas' table
df_CabVentas = DB_tables['CabVentas']

# Create a dataframe with the data from the 'ItemVentas' table
df_ItemVentas = DB_tables['ItemVentas']



# =====================================
#  ETL: DataBase "SMARTS"
#  Autoras: Laura Chaves, Fiorella Righelato, Daiana Gareis
#  GitHub: https://github.com/Laura-Chaves/Datawarehouse_SmartsTV
# =====================================


# =============
#  Bibliotecas
# =============

import pyodbc # Conexión con la base de datos
import configparser # Configuración de la base de datos
from sqlalchemy import create_engine # Creación de la conexión a la BD
import logging # Registro de errores
import pandas as pd # Manejo de datos
import numpy as np # Manejo de matrices
import re # expresiones regulares

#from modules.update_dimensions_table import updateDimensionTable, updateDimensionTableIntPK # Function to update dimensions tables




# ========================
# Configuración de conexión
# ========================

# Configuración de registro de errores
logging.basicConfig(filename='errores_etl.log', level=logging.ERROR, format='%(asctime)s %(message)s')


# Leer las credenciales de la base de datos desde el archivo secrets.ini
config = configparser.ConfigParser()
config.read('secrets.ini')


usuario = config['database']['username']
contraseña = config['database']['password']
host = config['database']['host']
puerto = config['database']['port']
nombre_bd_origen = config['database']['dbname']
nombre_bd_dw = config['datawarehouse']['dbname']



# ========================
# ETAPA 1: Extracción
# ========================

try:
    df = pd.read_csv('data/final_final_smart_tv_data.csv')
    print("Datos cargados exitosamente desde el archivo CSV.")
except Exception as e:
    logging.error(f"Error al cargar el archivo CSV: {e}")
    raise e

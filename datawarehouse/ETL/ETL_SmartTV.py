# =====================================
#  ETL: DataBase "SMARTS"
#  Autoras: Laura Chaves, Fiorella Righelato, Daiana Gareis
#  GitHub: https://github.com/Laura-Chaves/Datawarehouse_SmartsTV
# =====================================

# =============
#  Bibliotecas
# =============
<<<<<<< HEAD
import pyodbc # Conexión con la base de datos
=======

>>>>>>> a5c6b6cf0cce99474d0888dfbb91b15e36d62cf2
import configparser # Configuración de la base de datos
import logging # Registro de errores
import pyodbc # Conexión con la base de datos
from sqlalchemy import create_engine # Creación de la conexión a la BD
import pandas as pd # Manejo de datos
import numpy as np # Manejo de matrices
import re # expresiones regulares
#from modules.update_dimensions_table import updateDimensionTable, updateDimensionTableIntPK # Function to update dimensions tables

# ========================
# Configuración de conexión
# ========================

# Read the configuration from the 'secrets.ini' file (contains username and password)
config = configparser.ConfigParser()
config.read('datawarehouse/ETL/secrets.ini')

# Name of the ODBC DSN
dsn_smart = 'SMARTS'
username_smart = config['database']['username']
password_smart = config['database']['password']

# Configuration Data Warehouse
dsn_dw = 'smarts_datawarehouse'	
username_dw = config['datawarehouse']['username']
password_dw = config['datawarehouse']['password']


# ========================
# Configuración de conexión
# ========================

# Crear conexión a la base de datos SMART
conn_smart = pyodbc.connect(f'DSN={dsn_smart};UID={username_smart};PWD={password_smart}')
cursor_smart = conn_smart.cursor()
print("Conexión exitosa a la base de datos SMART.")

# Crear conexión al Data Warehouse
engine_dw = create_engine(f"postgresql://{username_dw}:{password_dw}@localhost/{dsn_dw}")
print("Conexión exitosa al Data Warehouse.")


# ========================
#        Extracción
# ========================

df = pd.read_csv('data/smarts_dataset.csv')

# ===================================
#  Transformaciones para Dimensiones
# ===================================

# Dimensión Ubicacion: Separar 'Localidad' en 'Ciudad' y 'Provincia'
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
ubicacion_df = df[['Ciudad', 'Provincia']].drop_duplicates().reset_index(drop=True)

# Dimensión Plataforma
plataforma_df = df[['Plataforma']].drop_duplicates().reset_index(drop=True)
plataforma_df = plataforma_df.rename(columns={'Plataforma': 'Nombre_plataforma'})
plataforma_df['Conexión'] = 'WiFi'  # Valor predeterminado para conexión

# Dimensión Tiempo
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
tiempo_df = df[['Dia', 'Mes', 'Año', 'Trimestre']].drop_duplicates().reset_index(drop=True)

# Dimensión Dispositivo
dispositivo_df = df[['Dispositivo', 'Sistema Operativo']].drop_duplicates().reset_index(drop=True)
dispositivo_df = dispositivo_df.rename(columns={'Dispositivo': 'Tipo_dispositivo', 'Sistema Operativo': 'Sistema_operativo'})

# =============================
# Transformación de la tabla de hechos `Consumo`
# =============================
# Unimos las dimensiones para construir la tabla de hechos `Consumo`
consumo_df = df.merge(ubicacion_df, on=['Ciudad', 'Provincia'], how='left') \
               .merge(plataforma_df, left_on='Plataforma', right_on='Nombre_plataforma', how='left') \
               .merge(tiempo_df, on=['Dia', 'Mes', 'Año', 'Trimestre'], how='left') \
               .merge(dispositivo_df, on=['Tipo_dispositivo', 'Sistema_operativo'], how='left')

# Calcular `Total_conexiones` y `Intentos_acceso_Plataformas`
consumo_df['Total_conexiones'] = consumo_df.groupby(['Ciudad', 'Provincia', 'Nombre_plataforma', 'Dia', 'Mes', 'Año', 'Trimestre', 'Tipo_dispositivo', 'Sistema_operativo'])['Conexion Exitosa'].transform('sum')
consumo_df['Intentos_acceso_Plataformas'] = consumo_df.groupby(['Ciudad', 'Provincia', 'Nombre_plataforma', 'Dia', 'Mes', 'Año', 'Trimestre', 'Tipo_dispositivo', 'Sistema_operativo'])['Conexion Exitosa'].transform('count')
consumo_df = consumo_df[['Ciudad', 'Provincia', 'Nombre_plataforma', 'Dia', 'Mes', 'Año', 'Trimestre', 'Tipo_dispositivo', 'Sistema_operativo', 'Total_conexiones', 'Intentos_acceso_Plataformas']].drop_duplicates().reset_index(drop=True)

# =============================
# Carga de datos al Data Warehouse usando `to_sql`
# =============================

# Insertar cada dimensión en el Data Warehouse
ubicacion_df.to_sql('Ubicacion', con=engine_dw, if_exists='append', index=False)
plataforma_df.to_sql('Plataforma', con=engine_dw, if_exists='append', index=False)
tiempo_df.to_sql('Tiempo', con=engine_dw, if_exists='append', index=False)
dispositivo_df.to_sql('Dispositivo', con=engine_dw, if_exists='append', index=False)
consumo_df.to_sql('Consumo', con=engine_dw, if_exists='append', index=False)

# Cerrar conexión a SMART
cursor_smart.close()
conn_smart.close()
print("Carga completada en el Data Warehouse.")
import pandas as pd
from sqlalchemy import create_engine

# Extracción
csv_file_path = 'repeated_smart_tv_data.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# Verificar los nombres de las columnas
print(df.columns)


df.rename(columns={'fecha': 'Fecha'}, inplace=True)

# Transformación
# Convertir la columna de fecha a formato datetime
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')

# Separar la columna de Localidad en Ciudad y Provincia
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)

# Crear nuevas columnas de tiempo
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter

# Eliminar columnas innecesarias
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

print(df.head())



# Carga
# Configuración de la conexión a la base de datos PostgreSQL
#db_connection_str = 'postgresql+psycopg2://usuario:contraseña@localhost/nombre_bd'
#db_connection = create_engine(db_connection_str)

# Insertar datos en las tablas correspondientes
#df.to_sql('nombre_de_la_tabla', db_connection, if_exists='replace', index=False)
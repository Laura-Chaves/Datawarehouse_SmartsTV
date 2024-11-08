import pandas as pd
from sqlalchemy import create_engine, text

# Extracción
csv_file_path = 'C:/Bases de Datos Multidimensionales/Datawarehouse_SmartsTV/data/smarts_dataset.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# Transformación
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y-%m-%d')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

# Crear columna "Total_conexiones"
grouped = df[df['Conexion Exitosa'] == 1].groupby(['ID Usuario', 'Ciudad', 'Trimestre', 'Dispositivo', 'Plataforma']).size().reset_index(name='Total_conexiones')
df = df.merge(grouped, on=['ID Usuario', 'Ciudad', 'Trimestre', 'Dispositivo', 'Plataforma'], how='left')
df['Total_conexiones'] = df['Total_conexiones'].fillna(0).astype(int)

# Conexión a la base de datos PostgreSQL
db_connection_str = 'postgresql+psycopg2://postgres:1234@localhost:5432/SMARTS'
engine = create_engine(db_connection_str)

# Función para insertar datos
def insert_data(engine, query, params):
    with engine.connect() as connection:
        connection.execute(text(query), params)

# Iterar sobre el DataFrame y realizar inserciones
for index, row in df.iterrows():
    # Insertar en la tabla 'ubicacion'
    insert_data(engine,
        """
        INSERT INTO ubicacion (ID_Ubicacion, Ciudad, Provincia)
        VALUES (DEFAULT, :ciudad, :provincia)
        ON CONFLICT (Ciudad, Provincia) DO NOTHING;
        """,
        {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}
    )

    # Insertar en la tabla 'plataforma'
    insert_data(engine,
        """
        INSERT INTO plataforma (ID_Plataforma, Nombre_plataforma, Conexion)
        VALUES (DEFAULT, :plataforma, :conexion)
        ON CONFLICT (Nombre_plataforma) DO NOTHING;
        """,
        {'plataforma': row['Plataforma'], 'conexion': row['Conexion Exitosa']}
    )

    # Insertar en la tabla 'dispositivo'
    insert_data(engine,
        """
        INSERT INTO dispositivo (ID_Dispositivo, Tipo_dispositivo, Sistema_operativo)
        VALUES (DEFAULT, :dispositivo, :sistema_operativo)
        ON CONFLICT (Tipo_dispositivo) DO NOTHING;
        """,
        {'dispositivo': row['Dispositivo'], 'sistema_operativo': row['Sistema Operativo']}
    )

    # Insertar en la tabla 'tiempo'
    insert_data(engine,
        """
        INSERT INTO tiempo (ID_Tiempo, Dia, Mes, Año, Trimestre)
        VALUES (DEFAULT, :dia, :mes, :año, :trimestre)
        ON CONFLICT (Dia, Mes, Año) DO NOTHING;
        """,
        {'dia': row['Dia'], 'mes': row['Mes'], 'año': row['Año'], 'trimestre': row['Trimestre']}
    )


print("Datos insertados correctamente.")

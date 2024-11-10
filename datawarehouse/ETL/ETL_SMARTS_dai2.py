# =====================================
#  ETL: DataBase "SMARTS"
#  Autoras: Laura Chaves, Fiorella Righelato, Daiana Gareis
#  GitHub: https://github.com/Laura-Chaves/Datawarehouse_SmartsTV
# =====================================


import pandas as pd
from sqlalchemy import create_engine, text

# Extracción
csv_file_path = 'Smart_TV_Data_v2.csv'
df = pd.read_csv(csv_file_path, delimiter=',')


# ===================================
#  Transformaciones para Dimensiones
# ===================================

df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y-%m-%d')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)


# Conexión a la base de datos PostgreSQL
db_connection_str = 'postgresql+psycopg2://postgres:123@localhost:5432/SMARTS'
engine = create_engine(db_connection_str)

# Función para insertar datos
def insert_data(engine, queries):
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for query, params in queries:
                    connection.execute(text(query), params)
                transaction.commit()  # Confirmar la transacción al final
            except Exception as e:
                transaction.rollback()
                print(f"Error al insertar datos: {e}")


queries = []


# ===================================
#  Inserciones a las dimensiones
# ===================================


for index, row in df.iterrows():
    # Instrucción SQL y parámetros para cada tabla
    queries.append((
        """
        INSERT INTO ubicacion (id_ubicacion, Ciudad, Provincia)
        VALUES (DEFAULT, :ciudad, :provincia)
        ON CONFLICT (Ciudad, Provincia) DO NOTHING;
        """,
        {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}
    ))
    
    queries.append((
        """
        INSERT INTO plataforma (id_plataforma, Nombre_plataforma, Conexion)
        VALUES (DEFAULT, :plataforma, :conexion)
        ON CONFLICT (Nombre_plataforma) DO NOTHING;
        """,
        {'plataforma': row['Plataforma'], 'conexion': row['Conexion Exitosa']}
    ))
    
    queries.append((
        """
        INSERT INTO dispositivo (ID_Dispositivo, Tipo_dispositivo, Sistema_operativo)
        VALUES (DEFAULT, :dispositivo, :sistema_operativo)
        ON CONFLICT (Tipo_dispositivo) DO NOTHING;
        """,
        {'dispositivo': row['Dispositivo'], 'sistema_operativo': row['Sistema Operativo']}
    ))
    
    queries.append((
        """
        INSERT INTO tiempo (ID_Tiempo, Dia, Mes, Año, Trimestre)
        VALUES (DEFAULT, :dia, :mes, :año, :trimestre)
        ON CONFLICT (Dia, Mes, Año) DO NOTHING;
        """,
        {'dia': row['Dia'], 'mes': row['Mes'], 'año': row['Año'], 'trimestre': row['Trimestre']}
   
    ))

# Llamar a la función para ejecutar todas las inserciones en una transacción
insert_data(engine, queries)

print("Datos insertados correctamente.")


# ==========================================
#  Csv creado para obtener total_conexiones
# ==========================================

# Paso 1: Filtrar solo las conexiones exitosas y agrupar para obtener Total_conexiones
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='Total_conexiones')

# Paso 2: Función para obtener el ID de cada dimensión
def get_dimension_id(query, params):
    with engine.connect() as connection:
        result = connection.execute(text(query), params).fetchone()
        return result[0] if result else None

# Paso 3: Generar las columnas con los IDs de las dimensiones
grouped_consumo['ID_Tiempo'] = grouped_consumo['Trimestre'].apply(
    lambda trimestre: get_dimension_id("SELECT ID_Tiempo FROM tiempo WHERE Trimestre = :trimestre LIMIT 1", {'trimestre': trimestre})
)

grouped_consumo['ID_Ubicacion'] = grouped_consumo.apply(
    lambda row: get_dimension_id("SELECT ID_Ubicacion FROM ubicacion WHERE Ciudad = :ciudad AND Provincia = :provincia LIMIT 1", {'ciudad': row['Ciudad'], 'provincia': row['Provincia']}), axis=1
)

grouped_consumo['ID_Plataforma'] = grouped_consumo['Plataforma'].apply(
    lambda plataforma: get_dimension_id("SELECT ID_Plataforma FROM plataforma WHERE Nombre_plataforma = :plataforma LIMIT 1", {'plataforma': plataforma})
)

grouped_consumo['ID_Dispositivo'] = grouped_consumo['Dispositivo'].apply(
    lambda dispositivo: get_dimension_id("SELECT ID_Dispositivo FROM dispositivo WHERE Tipo_dispositivo = :dispositivo LIMIT 1", {'dispositivo': dispositivo})
)

# Paso 4: Calcular el total de conexiones exitosas en el dataset original y en el dataset agrupado
total_conexiones_original = df[df['Conexion Exitosa'] == 1].shape[0]
total_conexiones_agrupado = grouped_consumo['Total_conexiones'].sum()

# Verificar si ambos totales coinciden
print(f"Total de conexiones exitosas en el dataset original: {total_conexiones_original}")
print(f"Total de 'Total_conexiones' en el dataset agrupado: {total_conexiones_agrupado}")

# Paso 5: Guardar el DataFrame `grouped_consumo` en un archivo CSV
grouped_consumo.to_csv('total_conexiones.csv', index=False)
print("Archivo CSV 'total_conexiones.csv' guardado con éxito.")

# ================================================================
#  Agregar total_conexiones a la tabla de hechos
#  junto con los id de cada dimension
# ================================================================

# Leer el archivo CSV que contiene los datos agregados con los IDs de las dimensiones
csv_file_path = 'total_conexiones.csv'
df_fact_consumo = pd.read_csv(csv_file_path)

# Función para insertar o actualizar datos en la tabla de hechos
def upsert_fact_table(engine, df):
    upsert_query = """
    INSERT INTO Consumo (ID_Tiempo, ID_Ubicacion, ID_Plataforma, ID_Dispositivo, Total_conexiones, Intentos_acceso_Plataformas)
    VALUES (:id_tiempo, :id_ubicacion, :id_plataforma, :id_dispositivo, :total_conexiones, 0)
    ON CONFLICT (ID_Tiempo, ID_Ubicacion, ID_Plataforma, ID_Dispositivo)
    DO UPDATE SET
    Total_conexiones = EXCLUDED.Total_conexiones;
    """
    
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for index, row in df.iterrows():
                    # Crear un diccionario con los valores de cada fila
                    params = {
                        'id_tiempo': int(row['ID_Tiempo']),
                        'id_ubicacion': int(row['ID_Ubicacion']),
                        'id_plataforma': int(row['ID_Plataforma']),
                        'id_dispositivo': int(row['ID_Dispositivo']),
                        'total_conexiones': int(row['Total_conexiones'])
                    }
                    
                    # Ejecutar la instrucción de upsert
                    connection.execute(text(upsert_query), params)

                transaction.commit()
                print("Datos insertados y/o actualizados correctamente en la tabla de hechos.")
            except Exception as e:
                transaction.rollback()
                print(f"Error al insertar o actualizar datos en la tabla de hechos: {e}")

# Llamar a la función para hacer el upsert
upsert_fact_table(engine, df_fact_consumo)


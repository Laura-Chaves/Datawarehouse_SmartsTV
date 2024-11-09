
import pandas as pd
from sqlalchemy import create_engine, text

# Extracción
csv_file_path = 'Smart_TV_Data_v2.csv'
df = pd.read_csv(csv_file_path, delimiter=',')

# Transformación
df['Fecha'] = pd.to_datetime(df['Fecha'], format='%Y-%m-%d')
df[['Ciudad', 'Provincia']] = df['Localidad'].str.split(', ', expand=True)
df['Dia'] = df['Fecha'].dt.day
df['Mes'] = df['Fecha'].dt.month
df['Año'] = df['Fecha'].dt.year
df['Trimestre'] = df['Fecha'].dt.quarter
df.drop(columns=['Fecha', 'Localidad'], inplace=True)

# Agrupar datos según las dimensiones en la tabla 'consumo'
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='Total_conexiones')


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

# Iterar sobre el DataFrame y realizar inserciones
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


# Filtramos solo las conexiones exitosas y agrupamos para obtener Total_conexiones
df_filtered = df[df['Conexion Exitosa'] == 1]
grouped_consumo = df_filtered.groupby(['Dispositivo', 'Ciudad', 'Provincia', 'Trimestre', 'Plataforma']).size().reset_index(name='Total_conexiones')

# Extraer los IDs correspondientes de cada dimensión
def get_dimension_id(query, params):
    with engine.connect() as connection:
        result = connection.execute(text(query), params).fetchone()
        return result[0] if result else None

# Generar un nuevo DataFrame con los IDs de las dimensiones y Total_conexiones
data_for_fact_table = []

for index, row in grouped_consumo.iterrows():
    # Obtener IDs para cada dimensión
    id_tiempo = get_dimension_id("SELECT ID_Tiempo FROM tiempo WHERE Trimestre = :trimestre LIMIT 1", {'trimestre': row['Trimestre']})
    id_ubicacion = get_dimension_id("SELECT ID_Ubicacion FROM ubicacion WHERE Ciudad = :ciudad AND Provincia = :provincia LIMIT 1", {'ciudad': row['Ciudad'], 'provincia': row['Provincia']})
    id_plataforma = get_dimension_id("SELECT ID_Plataforma FROM plataforma WHERE Nombre_plataforma = :plataforma LIMIT 1", {'plataforma': row['Plataforma']})
    id_dispositivo = get_dimension_id("SELECT ID_Dispositivo FROM dispositivo WHERE Tipo_dispositivo = :dispositivo LIMIT 1", {'dispositivo': row['Dispositivo']})
    
    # Añadir la combinación de IDs y el Total_conexiones al nuevo dataset
    data_for_fact_table.append({
        'ID_Tiempo': id_tiempo,
        'ID_Ubicacion': id_ubicacion,
        'ID_Plataforma': id_plataforma,
        'ID_Dispositivo': id_dispositivo,
        'Total_conexiones': row['Total_conexiones']
    })

# Crear un nuevo DataFrame para la tabla de hechos
df_fact_consumo = pd.DataFrame(data_for_fact_table)


# Guardar el DataFrame `grouped_consumo` en un archivo CSV
grouped_consumo.to_csv('total_conexiones.csv', index=False)






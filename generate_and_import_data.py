import csv
import random
from datetime import datetime, timedelta
import mysql.connector

# Función para generar fechas aleatorias
def generate_random_date(start_date, end_date):
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

# Configuración de las fechas
start_date_2024 = datetime(2024, 3, 1)
end_date_2024 = datetime(2024, 4, 30)

start_date_2023 = datetime(2023, 6, 1)
end_date_2023 = datetime(2023, 6, 30)

start_date_2021 = datetime(2021, 8, 1)
end_date_2021 = datetime(2021, 10, 31)

# Generar y guardar datos para cada cartera
def generate_data(filename, start_date, end_date):
    data = []
    for _ in range(300000):  # Generar 300,000 registros
        tipo_promesa = random.choice(['pago_a_plazo', 'pago_unico', 'reestructuracion'])
        monto = round(random.uniform(500, 3000), 2)
        fecha = generate_random_date(start_date, end_date).date()
        estado = random.choice(['pendiente', 'completada'])
        cliente_id = random.randint(1, 100)  # Asumiendo que tienes 100 clientes
        data.append([tipo_promesa, monto, fecha, estado, cliente_id])

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['tipo_promesa', 'monto', 'fecha', 'estado', 'cliente_id'])
        writer.writerows(data)

# Generar archivos CSV
generate_data('promises_serfinanza_2024.csv', start_date_2024, end_date_2024)
generate_data('promises_serfinanza_2023.csv', start_date_2023, end_date_2023)
generate_data('promises_tuya_2021.csv', start_date_2021, end_date_2021)

# Conexión a MySQL
def import_data_to_mysql(filename, table_name):
    conn = mysql.connector.connect(
        host='localhost',    # Cambia esto si tu base de datos está en otro servidor
        user='YULI',         # Tu usuario de MySQL
        password='26151229', # Tu contraseña de MySQL
        allow_local_infile=True  # Permitir la carga de archivos locales
    )
    cursor = conn.cursor()

    # Comando para importar el archivo CSV
    load_data_query = f"""
    LOAD DATA LOCAL INFILE '{filename}'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS
    (tipo_promesa, monto, fecha, estado, cliente_id);
    """
    
    try:
        cursor.execute(load_data_query)
        conn.commit()
        print(f"Datos importados a la tabla {table_name} desde {filename}.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
        conn.close()

# Importar archivos CSV a MySQL
import_data_to_mysql('promises_serfinanza_2024.csv', 'serfinanza_2024.promises')
import_data_to_mysql('promises_serfinanza_2023.csv', 'serfinanza_2023.promises')
import_data_to_mysql('promises_tuya_2021.csv', 'tuya_2021.promises')

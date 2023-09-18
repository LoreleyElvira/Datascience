import pandas as pd
import psycopg2
import requests
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime

api_key = '0ff735e506cdaba35c120dfd4baa220b'
uruguay_cities = ['Montevideo', 'Salto', 'Artigas', 'Rivera', 'Paysandú', 'Cerro Largo', 'Maldonado', 'Tacuarembó', 'Durazno',
                  'Treinta y tres', 'Soriano', 'Florida', 'Colonia', 'San José', 'Canelones', 'Lavalleja', 'Rocha']

all_city_data = []


mapeo_departamentos = {
    'Montevideo': 1,
    'Salto': 2,
    'Artigas': 3,
    'Rivera': 4,
    'Paysandu': 5,
    'Departamento de Cerro Largo': 6,
    'Departamento de Maldonado': 7,
    'Tacuarembó': 8,
    'Durazno': 9,
    'Treinta y Tres': 10,
    'Soriano Department': 11,
    'Departamento de Florida': 12,
    'Departamento de Colonia': 13,
    'San José': 14,
    'Departamento de Canelones': 15,
    'Departamento de Lavalleja': 16,
    'Rocha': 17
}

for city_name in uruguay_cities:
    url = f'http://api.openweathermap.org/data/2.5/forecast?q={city_name},UY&appid={api_key}&units=metric'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        daily_data = []

        for item in data['list']:
            timestamp = item['dt']
            date_time = datetime.fromtimestamp(timestamp)

            if date_time.hour == 12:
                temp = item['main']['temp']
                humidity = item['main']['humidity']
                description = item['weather'][0]['description']
                wind_speed = item['wind']['speed']
                name = data['city']['name']

                departamento_numero = mapeo_departamentos.get(name)

                row = {'Fecha': date_time, 'Temperatura (°C)': temp, 'Humedad (%)': humidity, 'Descripción': description, 'Velocidad del Viento (m/s)': wind_speed, 'Departamento Desc': name, 'Departamento': departamento_numero}
                daily_data.append(row)

        all_city_data.extend(daily_data)
    else:
        print(f"Error en la solicitud para {city_name}. Código de estado: {response.status_code}")

"""SQL: Creado en Redshift 

create table IF NOT EXISTS  departamentos  (
    numero_ciudad INT PRIMARY KEY,
    nombre_ciudad VARCHAR(50)
);
INSERT INTO departamentos(numero_ciudad, nombre_ciudad)
VALUES
    (1, 'Montevideo'),
    (2, 'Salto'),
    (3, 'Artigas'),
    (4, 'Rivera'),
    (5, 'Paysandú'),
    (6, 'Cerro Largo'),
    (7, 'Maldonado'),
    (8, 'Tacuarembó'),
    (9, 'Durazno'),
    (10, 'Treinta y tres'),
    (11, 'Soriano'),
    (12, 'Florida'),
    (13, 'Colonia'),
    (14, 'San José'),
    (15, 'Canelones'),
    (16, 'Lavalleja'),
    (17, 'Rocha');
       
 CREATE TABLE IF NOT EXISTS clima_uruguay (
    fecha_hora TIMESTAMP NULL,
    temperatura DECIMAL(5, 2),
    humedad DECIMAL(5, 2),
    descripcion VARCHAR(100),
    velocidad_viento DECIMAL(5, 2),
    departamento INT,
    PRIMARY KEY (fecha_hora, departamento),  
    FOREIGN KEY (departamento) REFERENCES departamentos(numero_ciudad)
);
"""


host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
dbname = 'data-engineer-database'
user = 'loreley_elvira_coderhouse'
with open("C:/Users/elcor/Documents/pwd_coder.txt", 'r') as f:
    pwd = f.read()

    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=dbname,
        user=user,
        password=pwd,
        port='5439'
    )

cur = conn.cursor()


table_name = 'clima_uruguay'


columns = ['fecha_hora', 'temperatura', 'humedad', 'descripcion', 'velocidad_viento', 'departamento']


new_records = []

for row in all_city_data:
    fecha_hora = row['Fecha']
    departamento_numero= row['Departamento']

    if departamento_numero is not None:

        cur.execute("SELECT COUNT(*) FROM clima_uruguay WHERE fecha_hora = %s AND departamento = %s", (fecha_hora, departamento_numero))
        existing_records_count = cur.fetchone()[0]

        if existing_records_count == 0:
           
            new_records.append((fecha_hora, row['Temperatura (°C)'], row['Humedad (%)'], row['Descripción'], 
                                row['Velocidad del Viento (m/s)'], departamento_numero))


if new_records:
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s)"
    cur.executemany(insert_sql, new_records)
    conn.commit()


conn.close()

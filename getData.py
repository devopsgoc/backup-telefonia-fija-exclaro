import paramiko
import os
import logging
from datetime import datetime, timezone
from influxdb import InfluxDBClient
from apscheduler.schedulers.blocking import BlockingScheduler

# Datos de conexión SFTP
sftp_host = os.getenv("D_SFTP_HOST") 
sftp_port = os.getenv("D_SFTP_PORT")
sftp_user = os.getenv("D_SFTP_USER") 
sftp_password = os.getenv("D_SFTP_PASSWORD")

# Datos de conexión a InfluxDB v1
influx_host = os.getenv("D_INFLUX_HOST")
influx_port = os.getenv("D_INFLUX_PORT") 
influx_user = os.getenv("D_INFLUX_USER")
influx_password = os.getenv("D_INFLUX_PASSWORD")
influx_db = os.getenv("D_INFLUX_DB")

# Hora y Minutos de Ejecución
hour_x = os.getenv("D_HOUR")
minute_x = os.getenv("D_MINUTE")

# Ruta base en el servidor SFTP
base_path = os.getenv("D_BASE_PATH") 

# Directorio local para descargar archivos
local_path = os.getenv("D_LOCAL_PATH") 

# Patrones de archivos a buscar
TIPOS_ARCHIVOS = [
    'ARCH_SOX_SS_',
    'ARCH_UMG_',
    'ARCH_VSBC_SSEE_',
    'ARCH_TITAN_'
]

# Configurar el logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def crear_local_path():
    if not os.path.exists(local_path):
        os.makedirs(local_path)
        logging.info(f"Directorio local creado: {local_path}")
    else:
        logging.info(f"Directorio local ya existe: {local_path}")

def conectar_sftp():
    try:
        logging.info("Conectando al servidor SFTP...")
        transport = paramiko.Transport((sftp_host, int(sftp_port)))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("Conexión SFTP establecida correctamente.")
        return sftp, transport
    except Exception as e:
        logging.error(f"Error al conectar al SFTP: {e}")
        raise

def obtener_archivos_mas_recientes(sftp, ano_mes_actual, fecha_actual_yyyymmdd):
    try:
        ruta_completa = os.path.join(base_path, ano_mes_actual)
        logging.info(f"Accediendo al directorio: {ruta_completa}")
        sftp.chdir(ruta_completa)
        archivos = sftp.listdir('.')
        archivos_filtrados = {}

        for tipo in TIPOS_ARCHIVOS:
            archivos_tipo = [archivo for archivo in archivos if archivo.startswith(tipo + fecha_actual_yyyymmdd)]
            if archivos_tipo:
                archivos_tipo.sort(reverse=True)
                archivos_filtrados[tipo] = archivos_tipo[0]
                logging.info(f"Archivo más reciente para {tipo}: {archivos_tipo[0]}")
            else:
                logging.warning(f"No se encontraron archivos para {tipo}")

        return archivos_filtrados
    except Exception as e:
        logging.error(f"Error al obtener archivos: {e}")
        raise

def descargar_archivos(sftp, archivos_filtrados):
    try:
        for tipo, archivo in archivos_filtrados.items():
            ruta_local = os.path.join(local_path, archivo)
            logging.info(f"Descargando {archivo} a {ruta_local}...")
            sftp.get(archivo, ruta_local)
            logging.info(f"Archivo {archivo} descargado correctamente.")
    except Exception as e:
        logging.error(f"Error al descargar archivos: {e}")
        raise

def conectar_influxdb():
    try:
        client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password, database=influx_db)
        logging.info("Conectado a InfluxDB correctamente.")
        return client
    except Exception as e:
        logging.error(f"Error al conectar a InfluxDB: {e}")
        raise

import os
import logging
from datetime import datetime, timezone

def procesar_y_subir_archivos_influxdb(client):
    try:
        puntos = []
        timestamp_actual = int(datetime.now(timezone.utc).timestamp() * 1e9)  
        for archivo in os.listdir(local_path):
            ruta_archivo = os.path.join(local_path, archivo)
            if os.path.isfile(ruta_archivo):
                logging.info(f"Procesando archivo: {archivo}")

                with open(ruta_archivo, 'r') as f:
                    for linea in f:
                        columnas = linea.strip().split(";")
                        if len(columnas) >= 3:
                            kpi = columnas[1]
                            estado_raw = columnas[2]
                            estado = 1 if estado_raw == "OK" else 2 if estado_raw == "NOK" else 0

                            punto = {
                                "measurement": "backup_data",
                                "time": timestamp_actual,  # 🔹 Todas las inserciones usan el mismo timestamp
                                "fields": {
                                    "kpi": kpi,
                                    "estado": estado
                                }
                            }
                            puntos.append(punto)
                
                logging.info(f"Archivo {archivo} procesado y listo para subir.")
                os.remove(ruta_archivo)
                logging.info(f"Archivo {archivo} eliminado.")
        if puntos:
            client.write_points(puntos)  # 🔹 Se suben todas las inserciones con el mismo timestamp
            logging.info(f"Se insertaron {len(puntos)} registros en InfluxDB con timestamp {timestamp_actual}.")
        else:
            logging.warning("No hay datos para insertar en InfluxDB.")
    except Exception as e:
        logging.error(f"Error al procesar archivos e insertar en InfluxDB: {e}")
        raise

def main():
    crear_local_path()
    fecha_actual = datetime.now()
    ano_mes_actual = fecha_actual.strftime('%Y-%m')
    fecha_actual_yyyymmdd = fecha_actual.strftime('%Y%m%d')
    sftp, transport, client_influxdb = None, None, None

    try:
        sftp, transport = conectar_sftp()
        archivos_filtrados = obtener_archivos_mas_recientes(sftp, ano_mes_actual, fecha_actual_yyyymmdd)
        if archivos_filtrados:
            descargar_archivos(sftp, archivos_filtrados)
        client_influxdb = conectar_influxdb()
        procesar_y_subir_archivos_influxdb(client_influxdb)
    except Exception as e:
        logging.error(f"Proceso fallido: {e}")
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        if client_influxdb:
            client_influxdb.close()
        logging.info("Proceso finalizado.")

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', hour=hour_x, minute=minute_x)
    logging.info("Programación iniciada.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Programador detenido.")

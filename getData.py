import paramiko
import os
import logging
from datetime import datetime
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

def conectar_influxdb():
    try:
        client = InfluxDBClient(
            host=influx_host,
            port=influx_port,
            username=influx_user,
            password=influx_password,
            database=influx_db
        )
        logging.info("Conectado a InfluxDB v1 correctamente.")
        return client
    except Exception as e:
        logging.error(f"Error al conectar a InfluxDB v1: {e}")
        raise

def procesar_y_subir_archivos_influxdb(client):
    try:
        puntos = []

        for archivo in os.listdir(local_path):
            ruta_archivo = os.path.join(local_path, archivo)
            if os.path.isfile(ruta_archivo):
                logging.info(f"Procesando archivo: {archivo}")
                with open(ruta_archivo, 'r') as f:
                    for linea in f:
                        columnas = linea.strip().split(";")
                        if len(columnas) >= 3:
                            segunda_columna = columnas[1]
                            tercera_columna = columnas[2]

                            tercera_columna_value = 1 if tercera_columna == "OK" else 2 if tercera_columna == "NOK" else 0

                            # Crear punto para InfluxDB v1
                            point = {
                                "measurement": "backup_data",
                                "tags": {
                                    "archivo": archivo
                                },
                                "fields": {
                                    "campo2": segunda_columna,
                                    "campo3": tercera_columna_value
                                },
                                "time": datetime.utcnow().isoformat()
                            }
                            puntos.append(point)

                logging.info(f"Archivo {archivo} procesado. Eliminando...")
                os.remove(ruta_archivo)
        
        if puntos:
            client.write_points(puntos)
            logging.info(f"Se insertaron {len(puntos)} registros en InfluxDB v1.")
        else:
            logging.warning("No hay datos para insertar en InfluxDB v1.")

    except Exception as e:
        logging.error(f"Error al procesar archivos e insertar en InfluxDB v1: {e}")
        raise

def main():
    client_influxdb = None

    try:
        client_influxdb = conectar_influxdb()
        procesar_y_subir_archivos_influxdb(client_influxdb)

    except Exception as e:
        logging.error(f"Proceso fallido: {e}")

    finally:
        if client_influxdb:
            client_influxdb.close()
            logging.info("Conexión InfluxDB v1 cerrada.")
        logging.info("Proceso finalizado.")

# Configuración del programador
if __name__ == "__main__":
    logger.info("Iniciando...")

    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', hour=hour_x, minute=minute_x)

    logger.info(f"Programación iniciada. La función se ejecutará todos los días a las {hour_x}:{minute_x}.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Programador detenido.")

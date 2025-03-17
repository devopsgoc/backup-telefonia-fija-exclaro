import paramiko
import os
import logging
from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from apscheduler.schedulers.blocking import BlockingScheduler

# Datos de conexión SFTP
sftp_host = os.getenv("D_SFTP_HOST") 
sftp_port = os.getenv("D_SFTP_PORT")
sftp_user = os.getenv("D_SFTP_USER") 
sftp_password = os.getenv("D_SFTP_PASSWORD")

# Datos de conexión a InfluxDB
influx_host = os.getenv("D_INFLUX_HOST")
influx_port = os.getenv("D_INFLUX_PORT") 
influx_token =  os.getenv("D_INFLUX_TOKEN")  # Token (usuario:contraseña)
influx_org = os.getenv("D_INFLUX_ORG") 
influx_bucket = os.getenv("D_INFLUX_BUCKET") 

# Hora y Minutos de Ejecucion
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
        logging.info("Conectando al servidor SFTP con ssh-rsa forzado...")
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        key = transport.get_remote_server_key()
        if key.get_name() != 'ssh-rsa':
            raise Exception(f"El servidor no soporta 'ssh-rsa', sino: {key.get_name()}")
        logging.info(f"Conectado correctamente usando algoritmo: {key.get_name()}")
        sftp = paramiko.SFTPClient.from_transport(transport)
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
        logging.info(f"{len(archivos)} archivos encontrados en el directorio remoto.")
        archivos_filtrados = {}

        for tipo in TIPOS_ARCHIVOS:
            archivos_tipo = [archivo for archivo in archivos if archivo.startswith(tipo + fecha_actual_yyyymmdd)]
            if archivos_tipo:
                archivos_tipo.sort(reverse=True)
                archivo_mas_reciente = archivos_tipo[0]
                archivos_filtrados[tipo] = archivo_mas_reciente
                logging.info(f"Archivo más reciente para {tipo}: {archivo_mas_reciente}")
            else:
                logging.warning(f"No se encontraron archivos para el tipo: {tipo}")

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
        client = InfluxDBClient(
            url=f"http://{influx_host}:{influx_port}",
            token=influx_token,
            org=influx_org
        )
        logging.info("Conectado a InfluxDB correctamente.")
        return client
    except Exception as e:
        logging.error(f"Error al conectar a InfluxDB: {e}")
        raise


def procesar_y_subir_archivos_influxdb(client):
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)  # <-- Aquí corregido

        for archivo in os.listdir(local_path):
            ruta_archivo = os.path.join(local_path, archivo)
            if os.path.isfile(ruta_archivo):
                logging.info(f"Procesando archivo: {archivo}")
                with open(ruta_archivo, 'r') as f:
                    for linea in f:
                        columnas = linea.strip().split(";")
                        if len(columnas) >= 3:
                            segunda_columna = columnas[1]

                            # Lógica para manejar la tercera columna
                            tercera_columna = columnas[2]
                            if tercera_columna == "OK":
                                tercera_columna_value = 1
                            elif tercera_columna == "NOK":
                                tercera_columna_value = 2
                            else:
                                tercera_columna_value = 0

                            # Crear el punto para InfluxDB
                            point = (
                                Point("backup_data")
                                .field("campo2", segunda_columna)
                                .field("campo3", tercera_columna_value)  # Cambiado para usar el valor convertido
                                .time(datetime.now(timezone.utc), WritePrecision.NS)  # Aquí está la corrección
                            )
                            write_api.write(bucket=influx_bucket, record=point)

                logging.info(f"Archivo {archivo} procesado e insertado en InfluxDB.")
                os.remove(ruta_archivo)
                logging.info(f"Archivo {archivo} eliminado del directorio local.")
        
        logging.info("Datos enviados correctamente a InfluxDB.")

    except Exception as e:
        logging.error(f"Error al procesar archivos e insertar en InfluxDB: {e}")
        raise

def main():
    crear_local_path()
    fecha_actual = datetime.now()
    ano_mes_actual = fecha_actual.strftime('%Y-%m')
    fecha_actual_yyyymmdd = fecha_actual.strftime('%Y%m%d')
    sftp, transport = None, None
    client_influxdb = None

    try:
        sftp, transport = conectar_sftp()
        archivos_filtrados = obtener_archivos_mas_recientes(sftp, ano_mes_actual, fecha_actual_yyyymmdd)

        if archivos_filtrados:
            descargar_archivos(sftp, archivos_filtrados)
        else:
            logging.warning("No se encontraron archivos para descargar.")

        # Conexión e inserción en InfluxDB
        client_influxdb = conectar_influxdb()
        procesar_y_subir_archivos_influxdb(client_influxdb)

    except Exception as e:
        logging.error(f"Proceso fallido: {e}")

    finally:
        if sftp:
            sftp.close()
            logging.info("Sesión SFTP cerrada.")
        if transport:
            transport.close()
            logging.info("Conexión SSH cerrada.")
        if client_influxdb:
            client_influxdb.close()
            logging.info("Conexión InfluxDB cerrada.")
        logging.info("Proceso finalizado.")


# Configuración del programador
if __name__ == "__main__":
    logger.info("Iniciando...")

    # Crear un programador
    scheduler = BlockingScheduler()

    # Programar la función todos los días a las 07:45
    scheduler.add_job(main, 'cron', hour=hour_x, minute=minute_x)

    logger.info("Programación iniciada. La función se ejecutará todos los días a las 07:45.")
    try:
        # Iniciar el programador
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Programador detenido.")
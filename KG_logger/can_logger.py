from google.oauth2 import service_account
import pytz
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import can
import logging
from pathlib import Path
import typer
import os
import time
from w1thermsensor import W1ThermSensor, SensorNotReadyError
import threading
import requests
import RPi.GPIO as GPIO

# Параметры для Google Drive API
SERVICE_ACCOUNT_FILE = '/home/logger/KG_logger/bamboo-reason-433311-m0-2f856dd03538.json'
FOLDER_ID = '1XV515xYP53G1e2EvSIF4Ec8tfusvlRlU'
SCOPES = ['https://www.googleapis.com/auth/drive']

timezone = pytz.timezone('Europe/Moscow')

# Аутентификация с использованием сервисного аккаунта
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Создание клиента Google Drive API
service = build('drive', 'v3', credentials=creds)

app = typer.Typer()

# Настройка GPIO для отслеживания состояния питания
GPIO.setmode(GPIO.BCM)
GPIO.setup(18, GPIO.IN)  # GPIO 18 настроен на вход для проверки напряжения

def check_internet(url="https://www.google.com", timeout=5):
    try:
        requests.get(url, timeout=timeout)
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False

def upload_file_to_gdrive(file_path, folder_id, mime_type='text/csv'):
    file_name = Path(file_path).name
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype=mime_type)
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        os.remove(file_path)  # Удаляем файл после успешной загрузки
        logging.info(f"File ID: {file.get('id')} uploaded to folder ID: {folder_id}.")
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to Google Drive: {e}")

def upload_pending_files(log_dir):
    log_files = sorted(Path(log_dir).glob("*.csv"))
    with ThreadPoolExecutor(max_workers=5) as executor:  # Используем пул потоков для параллельной загрузки
        futures = [executor.submit(upload_file_to_gdrive, log_file, FOLDER_ID) for log_file in log_files]
        for future in futures:
            future.result()  # Ждем завершения всех загрузок

def read_temperatures(sensors, sensor_count, interval, stop_event, temperatures):
    while not stop_event.is_set():
        for i in range(6):
            try:
                if i < sensor_count:
                    temperature = sensors[i].get_temperature()
                else:
                    temperature = "Unavailable"
            except SensorNotReadyError:
                temperature = "Unavailable"
            temperatures[i] = temperature
        time.sleep(interval)

@app.command()
def log_can_data(interface: str = typer.Argument("can0", help="CAN interface, e.g., can0"),
                 log_dir: str = typer.Argument("./logs", help="Directory to save log files"),
                 log_duration: int = typer.Argument(10, help="Log duration in minutes"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file'),
                 check_interval: int = typer.Option(60, help='Interval for checking internet connection in seconds'),
                 shutdown_delay: int = typer.Option(300, help='Delay before shutdown if no CAN data is received in seconds')
                 ):

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    log_number = 0
    log_start_time = datetime.now(timezone)
    last_can_data_time = datetime.now(timezone)
    last_power_on_time = datetime.now(timezone)
    stop_event = threading.Event()
    pending_uploads = []  # Список для хранения файлов, которые нужно загрузить

    def rotate_log_file():
        nonlocal log_number, log_start_time, log_dir
        log_number += 1
        log_start_time = datetime.now(timezone)
        timestamp = log_start_time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = Path(log_dir) / f"{log_name}_{timestamp}.csv"

        if logger.handlers:
            logger.removeHandler(logger.handlers[0])

        file_handler = logging.FileHandler(log_file, mode='w')
        logger.addHandler(file_handler)
        logger.info("Timestamp,ID,Ext,RTR,Dir,Bus,Len,Data,Temperature_1,Temperature_2,Temperature_3,Temperature_4,Temperature_5,Temperature_6,Power")
        return log_file

    log_file = rotate_log_file()

    sensors = W1ThermSensor.get_available_sensors()
    sensor_count = len(sensors)
    temperatures = ["Unavailable"] * 6

    # Запускаем поток для считывания данных с датчиков
    temp_thread = threading.Thread(target=read_temperatures, args=(sensors, sensor_count, 2, stop_event, temperatures))
    temp_thread.start()

    # Запускаем поток для проверки интернета и загрузки файлов
    def internet_check_loop():
        while not stop_event.is_set():
            if check_internet():
                # Загружаем все файлы из списка `pending_uploads`
                while pending_uploads:
                    file_to_upload = pending_uploads.pop(0)
                    upload_file_to_gdrive(file_to_upload, FOLDER_ID)
            time.sleep(check_interval)

    internet_thread = threading.Thread(target=internet_check_loop)
    internet_thread.start()

    try:
        bus = can.interface.Bus(channel=interface, interface='socketcan')

        while True:
            power_state = GPIO.input(16)
            power_status = "True" if power_state == 1 else "False"

            if power_state:
                last_power_on_time = datetime.now(timezone)

            msg = bus.recv(timeout=1.0)  # Устанавливаем таймаут на 1 секунду

            if msg:
                last_can_data_time = datetime.now(timezone)
            else:
                # Если нет данных, записываем "Unavailable" в лог
                msg = None

            # Если не получаем данные с CAN шины в течение заданного времени
            if datetime.now(timezone) - last_can_data_time >= timedelta(seconds=shutdown_delay):
                logger.warning("No CAN data for 5 minutes. Shutting down.")
                break

            data_str = ' '.join(format(byte, '02X') for byte in msg.data) if msg else "Unavailable"
            log_entry = f"{datetime.now(timezone).isoformat()},{hex(msg.arbitration_id) if msg else 'Unavailable'},{msg.is_extended_id if msg else 'Unavailable'},{msg.is_remote_frame if msg else 'Unavailable'},{msg.is_error_frame if msg else 'Unavailable'},{msg.channel if msg else 'Unavailable'},{msg.dlc if msg else 'Unavailable'},{data_str},{','.join(map(str, temperatures))},{power_status}"
            logger.info(log_entry)

            # Если прошло заданное количество времени, ротируем лог
            if datetime.now(timezone) - log_start_time >= timedelta(seconds=log_duration):
                pending_uploads.append(log_file)  # Добавляем старый файл в список для загрузки
                log_file = rotate_log_file()

            # Если питание отсутствует больше 5 минут, завершаем работу
            if not power_state and datetime.now(timezone) - last_power_on_time >= timedelta(seconds=shutdown_delay):
                logger.warning("Power lost for 5 minutes. Shutting down.")
                break

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received, saving and uploading log file.")
    finally:
        stop_event.set()  # Останавливаем потоки
        temp_thread.join()
        internet_thread.join()

        pending_uploads.append(log_file)
        while pending_uploads:
            file_to_upload = pending_uploads.pop(0)
            upload_file_to_gdrive(file_to_upload, FOLDER_ID)

        if logger.handlers:
            logger.handlers[0].close()

if __name__ == "__main__":
    app()

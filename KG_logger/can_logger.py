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
    with ThreadPoolExecutor(max_workers=5) as executor:
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

def check_power(pin, interval, stop_event, power_status):
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(pin, GPIO.IN)
    while not stop_event.is_set():
        power_status[0] = GPIO.input(pin) == GPIO.HIGH
        time.sleep(interval)

@app.command()
def log_can_data(interface: str = typer.Argument("can0", help="CAN interface, e.g., can0"),
                 log_dir: str = typer.Argument("./logs", help="Directory to save log files"),
                 log_duration: int = typer.Argument(10, help="Log duration in minutes"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file'),
                 check_interval: int = typer.Option(60, help='Interval for checking internet connection in seconds'),
                 power_pin: int = typer.Option(18, help='GPIO pin number for power monitoring')
                 ):

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    log_number = 0
    log_start_time = datetime.now(timezone)
    stop_event = threading.Event()
    pending_uploads = []

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
    power_status = [True]

    temp_thread = threading.Thread(target=read_temperatures, args=(sensors, sensor_count, 2, stop_event, temperatures))
    power_thread = threading.Thread(target=check_power, args=(power_pin, 1, stop_event, power_status))
    temp_thread.start()
    power_thread.start()

    last_power_time = time.time()

    def internet_check_loop():
        while not stop_event.is_set():
            if check_internet():
                while pending_uploads:
                    file_to_upload = pending_uploads.pop(0)
                    upload_file_to_gdrive(file_to_upload, FOLDER_ID)
            time.sleep(check_interval)

    internet_thread = threading.Thread(target=internet_check_loop)
    internet_thread.start()

    try:
        bus = can.interface.Bus(channel=interface, interface='socketcan')

        while True:
            msg = bus.recv()
            data_str = ' '.join(format(byte, '02X') for byte in msg.data)
            log_entry = f"{datetime.now(timezone).isoformat()},{hex(msg.arbitration_id)},{msg.is_extended_id},{msg.is_remote_frame},{msg.is_error_frame},{msg.channel},{msg.dlc},{data_str},{','.join(map(str, temperatures))},{power_status[0]}"
            logger.info(log_entry)

            if power_status[0] is False:
                if time.time() - last_power_time >= 300:
                    logging.warning("Power lost for 5 minutes, shutting down.")
                    break
            else:
                last_power_time = time.time()

            if datetime.now(timezone) - log_start_time >= timedelta(seconds=log_duration):
                pending_uploads.append(log_file)
                log_file = rotate_log_file()

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received, saving and uploading log file.")
        pending_uploads.append(log_file)
    finally:
        while pending_uploads:
            file_to_upload = pending_uploads.pop(0)
            upload_file_to_gdrive(file_to_upload, FOLDER_ID)

        stop_event.set()
        temp_thread.join()
        power_thread.join()
        internet_thread.join()
        GPIO.cleanup()

        if logger.handlers:
            logger.handlers[0].close()

if __name__ == "__main__":
    app()

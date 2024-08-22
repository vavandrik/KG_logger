from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import can
import logging
from pathlib import Path
import typer
import dropbox
from dropbox.files import WriteMode
import os
import time
from w1thermsensor import W1ThermSensor, SensorNotReadyError
import threading
import requests

app = typer.Typer()


def check_internet(url="https://www.google.com", timeout=5):
    try:
        requests.get(url, timeout=timeout)
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False


def upload_to_dropbox_async(log_file, dropbox_token, dropbox_path="/"):
    def upload_file():
        dbx = dropbox.Dropbox(dropbox_token)
        with open(log_file, "rb") as f:
            file_data = f.read()
            try:
                dbx.files_upload(file_data, dropbox_path + log_file.name, mode=WriteMode('overwrite'))
                os.remove(log_file)  # Удаляем файл после успешной загрузки
                logging.info(f"Uploaded {log_file} to Dropbox")
            except dropbox.exceptions.ApiError as e:
                logging.error(f"Failed to upload {log_file} to Dropbox: {e}")

    upload_thread = threading.Thread(target=upload_file)
    upload_thread.start()


def upload_pending_files(log_dir, dropbox_token, dropbox_path):
    log_files = sorted(Path(log_dir).glob("*.csv"))
    with ThreadPoolExecutor(max_workers=5) as executor:  # Используем пул потоков для параллельной загрузки
        futures = [executor.submit(upload_to_dropbox_async, log_file, dropbox_token, dropbox_path) for log_file in
                   log_files]
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
                 dropbox_token: str = typer.Option(..., help="Dropbox API OAuth2 token"),
                 dropbox_path: str = typer.Option("/", help="Dropbox path to upload files"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file'),
                 check_interval: int = typer.Option(60, help='Interval for checking internet connection in seconds')
                 ):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    log_number = 0
    log_start_time = datetime.now()
    stop_event = threading.Event()
    pending_uploads = []  # Список для хранения файлов, которые нужно загрузить

    def rotate_log_file():
        nonlocal log_number, log_start_time, log_dir
        log_number += 1
        log_start_time = datetime.now()
        timestamp = log_start_time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = Path(log_dir) / f"{log_name}_{timestamp}.csv"

        if logger.handlers:
            logger.removeHandler(logger.handlers[0])

        file_handler = logging.FileHandler(log_file, mode='w')
        logger.addHandler(file_handler)
        logger.info(
            "Timestamp,ID,Ext,RTR,Dir,Bus,Len,Data,Temperature_1,Temperature_2,Temperature_3,Temperature_4,Temperature_5,Temperature_6")
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
                    upload_to_dropbox_async(file_to_upload, dropbox_token, dropbox_path)
            time.sleep(check_interval)

    internet_thread = threading.Thread(target=internet_check_loop)
    internet_thread.start()

    try:
        bus = can.interface.Bus(channel=interface, interface='socketcan')

        while True:
            msg = bus.recv()  # Получаем сообщение с CAN

            data_str = ' '.join(format(byte, '02X') for byte in msg.data)
            log_entry = f"{datetime.now().isoformat()},{hex(msg.arbitration_id)},{msg.is_extended_id},{msg.is_remote_frame},{msg.is_error_frame},{msg.channel},{msg.dlc},{data_str},{','.join(map(str, temperatures))}"
            logger.info(log_entry)

            # Если прошло заданное количество времени, ротируем лог
            if datetime.now() - log_start_time >= timedelta(seconds=log_duration):
                pending_uploads.append(log_file)  # Добавляем старый файл в список для загрузки
                log_file = rotate_log_file()

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received, saving and uploading log file.")
        pending_uploads.append(log_file)
        while pending_uploads:
            file_to_upload = pending_uploads.pop(0)
            upload_to_dropbox_async(file_to_upload, dropbox_token, dropbox_path)
    finally:
        stop_event.set()  # Останавливаем потоки
        temp_thread.join()
        internet_thread.join()
        if logger.handlers:
            logger.handlers[0].close()


if __name__ == "__main__":
    app()

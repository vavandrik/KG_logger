from datetime import datetime, timedelta
import can
import logging
from pathlib import Path
import typer
import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.files import WriteMode
import os
import time
from w1thermsensor import W1ThermSensor, SensorNotReadyError
import threading

app = typer.Typer()

def upload_to_dropbox_async(log_files, dropbox_token, dropbox_path="/", stop_event=None):
    def upload_files():
        dbx = dropbox.Dropbox(dropbox_token)
        for log_file in log_files:
            if stop_event and stop_event.is_set():
                return
            with open(log_file, "rb") as f:
                file_data = f.read()
                try:
                    dbx.files_upload(file_data, dropbox_path + log_file.name, mode=WriteMode('overwrite'))
                    os.remove(log_file)  # Удаляем файл после успешной загрузки
                    logging.info(f"Uploaded {log_file} to Dropbox")
                except AuthError:
                    logging.error("Invalid Dropbox token. Please check your token and try again.")
                    return
                except ApiError as e:
                    logging.error(f"Failed to upload {log_file} to Dropbox: {e}")
                    return  # Прекращаем попытки загрузки при ошибке

    threading.Thread(target=upload_files).start()

def check_internet_connection(dropbox_token):
    try:
        dbx = dropbox.Dropbox(dropbox_token)
        dbx.users_get_current_account()  # Запрос к API Dropbox для проверки соединения
        return True
    except AuthError:
        logging.error("Invalid Dropbox token. Please check your token.")
        return False
    except Exception:
        return False

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
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file')
                 ):

    if not check_internet_connection(dropbox_token):
        logging.error("Failed to verify Dropbox token. Exiting...")
        return

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    log_number = 0
    log_start_time = datetime.now()

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
        logger.info("Timestamp,ID,Ext,RTR,Dir,Bus,Len,Data,Temperature_1,Temperature_2,Temperature_3,Temperature_4,Temperature_5,Temperature_6")
        return log_file

    log_file = rotate_log_file()

    sensors = W1ThermSensor.get_available_sensors()
    sensor_count = len(sensors)
    temperatures = ["Unavailable"] * 6
    stop_event = threading.Event()

    temp_thread = threading.Thread(target=read_temperatures, args=(sensors, sensor_count, 2, stop_event, temperatures))
    temp_thread.start()

    try:
        bus = can.interface.Bus(channel=interface, interface='socketcan')

        while True:
            msg = bus.recv()  # Получаем сообщение с CAN

            data_str = ' '.join(format(byte, '02X') for byte in msg.data)
            log_entry = f"{datetime.now().isoformat()},{hex(msg.arbitration_id)},{msg.is_extended_id},{msg.is_remote_frame},{msg.is_error_frame},{msg.channel},{msg.dlc},{data_str},{','.join(map(str, temperatures))}"
            logger.info(log_entry)

            if datetime.now() - log_start_time >= timedelta(minutes=log_duration):
                log_file = rotate_log_file()

            if check_internet_connection(dropbox_token):
                log_files = list(Path(log_dir).glob(f"{log_name}_*.csv"))
                upload_to_dropbox_async(log_files, dropbox_token, dropbox_path, stop_event)

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received, saving and uploading log files.")
        stop_event.set()  # Останавливаем поток считывания температур
        temp_thread.join()

        log_files = list(Path(log_dir).glob(f"{log_name}_*.csv"))
        upload_to_dropbox_async(log_files, dropbox_token, dropbox_path, stop_event)
    finally:
        if logger.handlers:
            logger.handlers[0].close()

if __name__ == "__main__":
    app()

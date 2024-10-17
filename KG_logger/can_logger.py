import minimalmodbus
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
import threading
import requests
import RPi.GPIO as GPIO

# Google Drive API Parameters
SERVICE_ACCOUNT_FILE = '/home/logger/KG_logger/bamboo-reason-433311-m0-2f856dd03538.json'
FOLDER_ID = '15nPujuibsb0ypqIhYA014Xx_KYAJG6bu'
SCOPES = ['https://www.googleapis.com/auth/drive']

timezone = pytz.timezone('Europe/Moscow')

# Authentication using service account
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Create Google Drive API client
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
        logging.info(f"File ID: {file.get('id')} uploaded to folder ID: {folder_id}.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to Google Drive: {e}")
        return False

def upload_pending_files(log_dir):
    log_files = sorted(Path(log_dir).glob("*.csv"))
    if not log_files:
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for log_file in log_files:
            # Skip files that are still being written to (have .tmp extension)
            if log_file.suffix == '.tmp':
                continue
            futures.append(executor.submit(upload_file_to_gdrive, log_file, FOLDER_ID))
        for future, log_file in zip(futures, log_files):
            success = future.result()
            if success:
                try:
                    os.remove(log_file)
                    logging.info(f"Deleted local file {log_file}")
                except Exception as e:
                    logging.error(f"Failed to delete local file {log_file}: {e}")

def read_rs485_temperatures(instrument, temp_registers, interval, stop_event, temperatures):
    while not stop_event.is_set():
        for i, reg in enumerate(temp_registers):
            try:
                raw_value = instrument.read_register(reg, 0, functioncode=3)
                temperature = raw_value / 10.0  # Assuming data is in tenths of degrees
                temperatures[i] = temperature
            except Exception as e:
                temperatures[i] = "Unavailable"
                logging.error(f"Error reading temperature from register {reg}: {e}")
        time.sleep(interval)

def configure_rs485_instrument():
    instrument = minimalmodbus.Instrument('/dev/ttySC0', 1)  # RS485 connected at /dev/ttySC0 with slave address 1
    instrument.serial.baudrate = 9600
    instrument.serial.bytesize = 8
    instrument.serial.parity = 'N'
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 1
    instrument.mode = minimalmodbus.MODE_RTU
    return instrument

def rotate_log_file(log_dir, log_name, logger):
    timestamp = datetime.now(timezone).strftime("%Y-%m-%d_%H-%M-%S")
    log_file = Path(log_dir) / f"{log_name}_{timestamp}.csv"
    tmp_log_file = log_file.with_suffix('.tmp')

    if logger.handlers:
        logger.removeHandler(logger.handlers[0])

    file_handler = logging.FileHandler(tmp_log_file, mode='w')
    logger.addHandler(file_handler)
    logger.info("Timestamp,ID,Ext,RTR,Dir,Bus,Len,Data,Temperature_1,Temperature_2,Temperature_3,Temperature_4,Power")

    return tmp_log_file

def finalize_log_file(tmp_log_file):
    # Close the current file handler
    for handler in logging.getLogger('CAN_Logger').handlers:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            logging.getLogger('CAN_Logger').removeHandler(handler)

    # Rename the temporary log file to the final .csv file
    final_log_file = tmp_log_file.with_suffix('.csv')
    os.rename(tmp_log_file, final_log_file)
    return final_log_file

@app.command()
def log_can_data(interface: str = typer.Argument("can0", help="CAN interface, e.g., can0"),
                 log_dir: str = typer.Argument("./logs", help="Directory to save log files"),
                 log_duration: int = typer.Argument(1, help="Log duration in minutes"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file'),
                 check_interval: int = typer.Option(30, help='Interval for checking internet connection in seconds')
                 ):

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    stop_event = threading.Event()

    GPIO.setmode(GPIO.BCM)
    GPIO.setup(16, GPIO.IN)
    GPIO.setup(26, GPIO.OUT)

    log_file = rotate_log_file(log_dir, log_name, logger)
    log_start_time = datetime.now(timezone)

    # RS485 configuration for temperature readings
    instrument = configure_rs485_instrument()
    temp_registers = [160, 161, 162, 163]
    temperatures = ["Unavailable"] * 4

    temp_thread = threading.Thread(target=read_rs485_temperatures, args=(instrument, temp_registers, 1, stop_event, temperatures))
    temp_thread.start()

    def internet_check_loop():
        while not stop_event.is_set():
            if check_internet():
                upload_pending_files(log_dir)
            time.sleep(check_interval)

    internet_thread = threading.Thread(target=internet_check_loop)
    internet_thread.start()

    can_unavailable_start = None
    power_lost_start = None

    try:
        bus = can.interface.Bus(channel=interface, interface='socketcan')

        while True:
            power_state = GPIO.input(16) == GPIO.HIGH

            if not power_state:
                if power_lost_start is None:
                    power_lost_start = datetime.now()
            else:
                power_lost_start = None

            msg = bus.recv(timeout=0.3)
            if msg is None:
                if can_unavailable_start is None:
                    can_unavailable_start = datetime.now()
                elif datetime.now() - can_unavailable_start > timedelta(minutes=5) and power_lost_start and datetime.now() - power_lost_start > timedelta(minutes=5):
                    logger.warning("Power and CAN lost for more than 5 minutes, stopping.")
                    break
                data_str = "CAN Unavailable"
            else:
                can_unavailable_start = None
                data_str = ' '.join(format(byte, '02X') for byte in msg.data)

            log_entry = f"{datetime.now(timezone).isoformat()},{hex(msg.arbitration_id) if msg else 'N/A'},{msg.is_extended_id if msg else 'N/A'},{msg.is_remote_frame if msg else 'N/A'},{msg.is_error_frame if msg else 'N/A'},{msg.channel if msg else 'N/A'},{msg.dlc if msg else 'N/A'},{data_str},{','.join(map(str, temperatures))},{power_state}"
            logger.info(log_entry)

            # Rotate log if duration is exceeded
            if datetime.now(timezone) - log_start_time >= timedelta(minutes=log_duration):
                # Finalize the current log file
                final_log_file = finalize_log_file(log_file)
                # Start a new log file
                log_file = rotate_log_file(log_dir, log_name, logger)
                log_start_time = datetime.now(timezone)

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received, saving and uploading log file.")
    finally:
        # Finalize and upload the last log file
        final_log_file = finalize_log_file(log_file)  # Use log_file here
        upload_pending_files(log_dir)
        stop_event.set()  # Stop threads
        temp_thread.join()
        internet_thread.join()
        GPIO.output(26, GPIO.HIGH)  # Set GPIO 26 to HIGH
        time.sleep(10)
        GPIO.output(26, GPIO.LOW)  # Reset GPIO 26
        GPIO.cleanup()  # Clean up GPIO
        if logger.handlers:
            logger.handlers[0].close()

if __name__ == "__main__":
    app()

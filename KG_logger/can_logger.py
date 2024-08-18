from datetime import datetime

import can
import logging
from pathlib import Path
import typer
import dropbox
from dropbox.files import WriteMode
import os
import csv

app = typer.Typer()

@app.command()
def log_can_data(interface: str = typer.Argument("can0", help="CAN interface, e.g., can0"),
                 log_dir: str = typer.Argument("./logs", help="Directory to save log files"),
                 max_file_size: int = typer.Argument(10, help="Maximum log file size in MB"),
                 dropbox_token: str = typer.Option(..., help="Dropbox API OAuth2 token"),
                 dropbox_path: str = typer.Option("/", help="Dropbox path to upload files"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file')
                 ):
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Формируем базовое имя файла
    if log_name:
        log_file_base = f"{log_name}_{current_time}"
    else:
        log_file_base = f"can_log_{current_time}"

    log_file = Path(log_dir) / f"{log_file_base}.csv"
    log_number = 1
    current_log_size = 0

    # Настраиваем логгер
    logger = logging.getLogger("CAN_Logger")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_file, mode='w')
    logger.addHandler(file_handler)

    # Пишем заголовок CSV файла
    with open(log_file, 'w', newline='') as csvfile:
        log_writer = csv.writer(csvfile)
        log_writer.writerow(['Time', 'ID', 'DLC', 'Data'])

    def rotate_log_file():
        nonlocal log_number, current_log_size, log_file
        log_number += 1
        current_log_size = 0
        log_file = Path(log_dir) / f"{log_file_base}_{log_number}.csv"
        logger.removeHandler(logger.handlers[0])
        file_handler = logging.FileHandler(log_file, mode='w')
        logger.addHandler(file_handler)

        # Пишем заголовок в новый файл
        with open(log_file, 'w', newline='') as csvfile:
            log_writer = csv.writer(csvfile)
            log_writer.writerow(['Time', 'ID', 'DLC', 'Data'])

        return log_file

    log_file = rotate_log_file()  # Инициализация первого файла

    try:
        bus = can.interface.Bus(channel=interface, bustype='socketcan')
        while True:
            message = bus.recv()
            if message:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                with open(log_file, 'a', newline='') as csvfile:
                    log_writer = csv.writer(csvfile)
                    log_writer.writerow([timestamp, hex(message.arbitration_id), message.dlc, message.data.hex()])
                    current_log_size += len(str(message.data))

                if current_log_size >= max_file_size * 1024 * 1024:
                    old_log_file = log_file
                    log_file = rotate_log_file()

                    # Загрузка старого файла в Dropbox
                    if dropbox_token and dropbox_path:
                        with open(old_log_file, 'rb') as f:
                            dbx = dropbox.Dropbox(dropbox_token)
                            dbx.files_upload(f.read(), f"{dropbox_path}/{old_log_file.name}")

    except can.CanError as e:
        logger.error(f"CAN Error: {e}")
    except dropbox.exceptions.ApiError as e:
        logger.error(f"Dropbox API Error: {e}")
    finally:
        if bus:
            bus.shutdown()

if __name__ == "__main__":
    app()

import argparse
import os
import can
import logging
from pathlib import Path
from datetime import datetime
import dropbox
import csv


def log_can_data(interface, log_dir, max_file_size, dropbox_token=None, dropbox_path=None, log_name=None):
    # Получаем текущее время для имени файла
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
    parser = argparse.ArgumentParser(description='Log CAN data to file and upload to Dropbox.')
    parser.add_argument('interface', type=str, help='CAN interface to use, e.g., can0')
    parser.add_argument('log_dir', type=str, help='Directory to store log files')
    parser.add_argument('max_file_size', type=int, help='Maximum size of log file in MB before rotation')
    parser.add_argument('--dropbox-token', type=str, help='Dropbox API token')
    parser.add_argument('--dropbox-path', type=str, help='Path in Dropbox to upload files')
    parser.add_argument('--log-name', type=str, help='Optional base name for the log file')

    args = parser.parse_args()

    log_can_data(args.interface, args.log_dir, args.max_file_size, args.dropbox_token, args.dropbox_path, args.log_name)

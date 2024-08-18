from datetime import datetime
import can
import logging
from pathlib import Path
import typer
import dropbox
from dropbox.files import WriteMode
import os

app = typer.Typer()

def upload_to_dropbox(log_file, dropbox_token, dropbox_path="/"):
    dbx = dropbox.Dropbox(dropbox_token)
    with open(log_file, "rb") as f:
        file_data = f.read()
        try:
            dbx.files_upload(file_data, dropbox_path + log_file.name, mode=WriteMode('overwrite'))
            os.remove(log_file)  # Удаляем файл после успешной загрузки
            logging.info(f"Uploaded {log_file} to Dropbox")
        except dropbox.exceptions.ApiError as e:
            logging.error(f"Failed to upload {log_file} to Dropbox: {e}")

@app.command()
def log_can_data(interface: str = typer.Argument("can0", help="CAN interface, e.g., can0"),
                 log_dir: str = typer.Argument("./logs", help="Directory to save log files"),
                 max_file_size: int = typer.Argument(10, help="Maximum log file size in MB"),
                 dropbox_token: str = typer.Option(..., help="Dropbox API OAuth2 token"),
                 dropbox_path: str = typer.Option("/", help="Dropbox path to upload files"),
                 log_name: str = typer.Option("can_log", help='Optional base name for the log file')
                 ):

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('CAN_Logger')

    log_number = 0
    current_log_size = 0

    def rotate_log_file(log_name: str):
        nonlocal log_number, current_log_size, log_dir
        log_number += 1
        current_log_size = 0
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = Path(log_dir) / f"{log_name}_{timestamp}.csv"

        # Проверяем, есть ли обработчики, прежде чем удалять
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])

        file_handler = logging.FileHandler(log_file, mode='w')
        logger.addHandler(file_handler)

        # Пишем заголовок CSV файла для совместимости с SavvyCAN
        file_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.info("Time,ID,Ext,RTR,Dir,Bus,Len,Data")

        return log_file

    log_file = rotate_log_file(log_name)

    try:
        bus = can.interface.Bus(channel=interface, bustype='socketcan')

        while True:
            msg = bus.recv()  # Получаем сообщение с CAN
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            msg_data = ' '.join(f'{byte:02X}' for byte in msg.data)
            logger.info(f"{timestamp},{msg.arbitration_id:X},{int(msg.is_extended_id)},{int(msg.is_remote_frame)},"
                        f"{'Rx' if msg.is_rx else 'Tx'},{msg.channel},{msg.dlc},{msg_data}")

            current_log_size += len(str(msg))

            if current_log_size >= max_file_size * 1024 * 1024:
                upload_to_dropbox(log_file, dropbox_token, dropbox_path)  # Загружаем файл в Dropbox
                log_file = rotate_log_file(log_name)  # Переход на новый файл

    except (OSError, can.CanError) as e:
        logger.error(f"Error with CAN interface: {e}")

if __name__ == "__main__":
    app()

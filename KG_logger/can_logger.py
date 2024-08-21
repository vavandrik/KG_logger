from datetime import datetime
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
import asyncio
import aiohttp

app = typer.Typer()

async def upload_to_dropbox_async(log_file, dropbox_token, dropbox_path="/"):
    url = "https://content.dropboxapi.com/2/files/upload"
    headers = {
        "Authorization": f"Bearer {dropbox_token}",
        "Dropbox-API-Arg": f'{{"path": "{dropbox_path}{log_file.name}", "mode": "overwrite"}}',
        "Content-Type": "application/octet-stream"
    }

    async with aiohttp.ClientSession() as session:
        with open(log_file, "rb") as f:
            file_data = f.read()
        async with session.post(url, headers=headers, data=file_data) as resp:
            if resp.status == 200:
                os.remove(log_file)  # Удаляем файл после успешной загрузки
                logging.info(f"Uploaded {log_file} to Dropbox")
            else:
                logging.error(f"Failed to upload {log_file} to Dropbox: {resp.status}")

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

    def rotate_log_file():
        nonlocal log_number, current_log_size, log_dir
        log_number += 1
        current_log_size = 0
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = Path(log_dir) / f"{log_name}_{timestamp}.csv"

        if logger.handlers:
            logger.removeHandler(logger.handlers[0])

        file_handler = logging.FileHandler(log_file, mode='w')
        logger.addHandler(file_handler)
        # Добавляем заголовок CSV
        logger.info("Timestamp,ID,Ext,RTR,Dir,Bus,Len,Data,Temperature_1,Temperature_2,Temperature_3,Temperature_4,Temperature_5,Temperature_6")
        return log_file

    log_file = rotate_log_file()

    sensors = W1ThermSensor.get_available_sensors()
    sensor_count = len(sensors)
    temperatures = ["Unavailable"] * 6
    stop_event = threading.Event()

    # Запускаем поток для считывания данных с датчиков
    temp_thread = threading.Thread(target=read_temperatures, args=(sensors, sensor_count, 2, stop_event, temperatures))
    temp_thread.start()

    async def process_can_data():
        nonlocal log_file, current_log_size

        try:
            bus = can.interface.Bus(channel=interface, interface='socketcan')

            while True:
                msg = bus.recv()  # Получаем сообщение с CAN

                data_str = ' '.join(format(byte, '02X') for byte in msg.data)
                log_entry = f"{datetime.now().isoformat()},{hex(msg.arbitration_id)},{msg.is_extended_id},{msg.is_remote_frame},{msg.is_error_frame},{msg.channel},{msg.dlc},{data_str},{','.join(map(str, temperatures))}"
                logger.info(log_entry)
                current_log_size += len(log_entry)

                if current_log_size >= max_file_size * 1024 * 1024:
                    await upload_to_dropbox_async(log_file, dropbox_token, dropbox_path)
                    log_file = rotate_log_file()

        except (OSError, can.CanError) as e:
            logger.error(f"Error with CAN interface: {e}")
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt received, saving and uploading log file.")
            await upload_to_dropbox_async(log_file, dropbox_token, dropbox_path)
        finally:
            stop_event.set()  # Останавливаем поток считывания температур
            temp_thread.join()
            if logger.handlers:
                logger.handlers[0].close()

    asyncio.run(process_can_data())

if __name__ == "__main__":
    app()

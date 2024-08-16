# CAN Logger for Raspberry Pi with MCP2515

This project logs CAN bus data from an MCP2515 CAN controller connected to a Raspberry Pi. It logs data to files and uploads the logs to a cloud storage when the file size limit is reached.

## Requirements

- Python 3.6+
- MCP2515 CAN Controller
- Raspberry Pi

## Installation

Install the required Python packages:

```bash
poetry install

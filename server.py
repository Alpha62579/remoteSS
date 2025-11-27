import asyncio
import subprocess
from bless import (
    BlessServer, BlessGATTCharacteristic,
    GATTCharacteristicProperties, GATTAttributePermissions
)
from datetime import datetime
from PIL import ImageGrab
import os
import io
import re

SS_FOLDER = os.path.join(os.getenv("LOCALAPPDATA"), "RemoteSS")
if not os.path.exists(SS_FOLDER):
    os.makedirs(SS_FOLDER)

SERVICE_UUID = "0d1a3c30-10ff-485a-9d37-3ac2501eb953"
COMMAND_CHAR_UUID = "0d1a3c31-10ff-485a-9d37-3ac2501eb953"
RESPONSE_CHAR_UUID = "0d1a3c32-10ff-485a-9d37-3ac2501eb953"
DATA_CHAR_UUID = "0d1a3c33-10ff-485a-9d37-3ac2501eb953"  # For large binary data

ANSI_ESCAPE = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]')

server_instance = None
MTU_SIZE = 512

def write_request(characteristic: BlessGATTCharacteristic, value: bytearray, **kwargs):
    """Handle incoming commands from client"""
    command = value.decode('utf-8')
    print(f"Client sent command: {command}")
    
    # Execute command and get result
    result, result_type, binary_data = execute_command(command)
    
    if server_instance and server_instance.is_connected:
        response_char = server_instance.get_characteristic(RESPONSE_CHAR_UUID)
        result_bytes = result.encode('utf-8') if isinstance(result, str) else b""
        if result_type in ("text", "error") and len(result_bytes) > MTU_SIZE:
            metadata = f"{result_type}_chunked:{len(result_bytes)}"
        else:
            metadata = f"{result_type}:{result}"
        response_char.value = bytearray(metadata.encode('utf-8'))
        server_instance.update_value(SERVICE_UUID, RESPONSE_CHAR_UUID)
        print(f"Sent response: {metadata[:100]}...")
        
        if binary_data:
            loop.create_task(send_binary_data(binary_data))
        elif result_type in ("text", "error") and len(result_bytes) > MTU_SIZE:
            loop.create_task(send_text_data(result_bytes))

async def send_binary_data(data: bytes):
    if not server_instance or not server_instance.is_connected:
        return
    
    data_char = server_instance.get_characteristic(DATA_CHAR_UUID)
    total_size = len(data)
    chunks = (total_size + MTU_SIZE - 1) // MTU_SIZE
    
    print(f"Sending {total_size} bytes in {chunks} chunks...")
    
    for i in range(0, total_size, MTU_SIZE):
        chunk = data[i:i + MTU_SIZE]
        data_char.value = bytearray(chunk)
        server_instance.update_value(SERVICE_UUID, DATA_CHAR_UUID)
        await asyncio.sleep(0.01)  # Small delay between chunks
        
        if (i // MTU_SIZE) % 10 == 0:
            print(f"Progress: {i}/{total_size} bytes sent")
    
    data_char.value = bytearray(b"")
    server_instance.update_value(SERVICE_UUID, DATA_CHAR_UUID)
    print("Binary data transfer complete")


async def send_text_data(data: bytes):
    if not server_instance or not server_instance.is_connected:
        return

    data_char = server_instance.get_characteristic(DATA_CHAR_UUID)
    total_size = len(data)
    # Reserve some bytes for a small ascii header per chunk
    header_reserved = 64
    per_chunk = max(1, MTU_SIZE - header_reserved)
    chunks = (total_size + per_chunk - 1) // per_chunk

    print(f"Sending text {total_size} bytes in {chunks} chunks...")

    seq = 1
    for i in range(0, total_size, per_chunk):
        chunk = data[i:i + per_chunk]
        header = f"{seq}/{chunks}:".encode('utf-8')
        data_char.value = bytearray(header + chunk)
        server_instance.update_value(SERVICE_UUID, DATA_CHAR_UUID)
        await asyncio.sleep(0.02)  # Small delay between chunks
        if seq % 10 == 0:
            print(f"Text progress: {i}/{total_size} bytes sent (chunk {seq}/{chunks})")
        seq += 1

    # Send end-of-data marker (empty notification)
    data_char.value = bytearray(b"")
    server_instance.update_value(SERVICE_UUID, DATA_CHAR_UUID)
    print("Text data transfer complete")

def execute_command(command):
    try:
        print(f"Executing: {command}")
        
        if command == "screenshot":
            image = ImageGrab.grab()
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            
            # Save to disk
            filepath = os.path.join(SS_FOLDER, f"{timestamp}.png")
            image.save(filepath)
            
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_bytes = img_byte_arr.getvalue()
            
            return (f"Screenshot captured: {timestamp}.png, {len(img_bytes)} bytes", 
                    "image", img_bytes)

        elif command.lower().startswith("launch "):
            program = command[7:].strip()
            subprocess.Popen(program, shell=True)
            return (f"Launched: {program}", "text", None)

        else:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                return (ANSI_ESCAPE.sub('', output) if output else "Command completed (no output)", 
                        "text", None)
            else:
                return (f"Error (code {result.returncode}): {ANSI_ESCAPE.sub('', result.stderr.strip())}", 
                        "error", None)

    except subprocess.TimeoutExpired:
        return ("ERROR: Command timed out (>10s)", "error", None)
    except Exception as e:
        return (f"ERROR: {str(e)}", "error", None)

async def run_server(loop):
    global server_instance
    
    # Define GATT structure
    gatt = {
        SERVICE_UUID: {
            COMMAND_CHAR_UUID: {
                "Properties": GATTCharacteristicProperties.write,
                "Permissions": GATTAttributePermissions.writeable,
                "Value": bytearray(b""),
            },
            RESPONSE_CHAR_UUID: {
                "Properties": (GATTCharacteristicProperties.read | 
                             GATTCharacteristicProperties.notify),
                "Permissions": GATTAttributePermissions.readable,
                "Value": bytearray(b"Ready"),
            },
            DATA_CHAR_UUID: {
                "Properties": (GATTCharacteristicProperties.read | 
                             GATTCharacteristicProperties.notify),
                "Permissions": GATTAttributePermissions.readable,
                "Value": bytearray(b""),
            },
        },
    }

    server = BlessServer(name="RemoteSS", loop=loop)
    server.write_request_func = write_request
    server_instance = server
    
    await server.add_gatt(gatt)
    await server.start()

    print("RemoteSS running. Waiting for commands...")
    
    # Keep server alive
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_server(loop))
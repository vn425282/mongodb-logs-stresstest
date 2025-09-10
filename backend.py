import eventlet
eventlet.monkey_patch()

import os
import shutil
import tempfile
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from multiprocessing import Process, Queue
import motor.motor_asyncio
import asyncio

from stress_test_logic import run_stress_test

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
test_process = None

@app.route('/')
def index():
    """Serves the main HTML user interface."""
    return render_template('index.html')

def queue_emitter(queue):
    """Listens to a queue for messages from the test process and emits them to the frontend."""
    while True:
        message = queue.get()
        if message is None:
            break
        socketio.emit('test_update', message)

@socketio.on('start_test')
def handle_start_test(data):
    """Starts the stress test in a separate, dedicated process."""
    global test_process
    if test_process and test_process.is_alive():
        emit('test_error', {'error': 'A test is already running.'})
        return

    params = data['params']
    files = data['files']

    try:
        temp_dir = tempfile.mkdtemp()
        for file_data in files:
            with open(os.path.join(temp_dir, file_data['name']), 'w', encoding='utf-8') as f:
                f.write(file_data['content'])
        params['templates_folder'] = temp_dir
    except Exception as e:
        emit('test_error', {'error': f'Failed to handle uploaded files: {e}'})
        return

    # NEW: Determine the number of CPU cores to use for data generation.
    # We default to half the available cores to leave resources for the OS and network tasks.
    total_cores = os.cpu_count() or 1
    params['generator_cores'] = params.get('generator_cores') or max(1, total_cores // 2)

    queue = Queue()
    test_process = Process(target=run_stress_test, args=(params, queue))
    test_process.start()
    
    socketio.start_background_task(target=queue_emitter, queue=queue)
    emit('test_started', {'message': f'Test started with {params["workers"]} writers and {params["generator_cores"]} generator cores.'})

@socketio.on('stop_test')
def handle_stop_test():
    """Terminates the running stress test process."""
    global test_process
    if test_process and test_process.is_alive():
        test_process.terminate()
        test_process.join()
        test_process = None
        emit('test_stopped', {'message': 'Test stopped by user.'})
    else:
        emit('test_error', {'error': 'No test is currently running.'})

@socketio.on('test_mdb_connection')
def handle_mdb_test(data):
    """Handles the MongoDB connection test."""
    uri = data.get('mdb_uri')
    
    async def do_test():
        client = None
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(uri, serverSelectionTimeoutMS=3000)
            await client.admin.command('ping')
            socketio.emit('mdb_connection_result', {'status': 'success', 'message': 'MongoDB connection successful!'})
        except Exception as e:
            socketio.emit('mdb_connection_result', {'status': 'error', 'message': f'Connection failed: {e}'})
        finally:
            if client:
                client.close()
    
    def run_async_wrapper():
        asyncio.run(do_test())

    socketio.start_background_task(run_async_wrapper)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)

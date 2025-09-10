import eventlet
eventlet.monkey_patch()

import os
import shutil
import tempfile
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from multiprocessing import Process, Queue, cpu_count
import motor.motor_asyncio
import asyncio

# Import the core logic from the separate file
from stress_test_logic import run_stress_test

# Automatically calculate the number of CPU cores to use for log generation.
# Uses half of the available cores to prevent system freeze.
GENERATOR_CORES = max(1, cpu_count() // 2)

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
test_process = None

@app.route('/')
def index():
    """Serves the main HTML user interface."""
    return render_template('index.html')

def queue_emitter(queue):
    """
    A target function for a background task. 
    It continuously listens to a queue for messages from the stress test process 
    and emits them to the frontend via SocketIO.
    """
    while True:
        message = queue.get()
        if message is None: # A None value is a sentinel to signal the end.
            break
        socketio.emit('test_update', message)

@socketio.on('start_test')
def handle_start_test(data):
    """
    Handles the 'start_test' event from the client.
    Sets up the test parameters and starts the stress test in a separate process.
    """
    global test_process
    if test_process and test_process.is_alive():
        emit('test_error', {'error': 'A test is already running.'})
        return

    params = data['params']
    
    # --- FIXED LOGIC ---
    # The backend now checks the payload from the UI to determine the mode,
    # making the UI toggle the single source of truth.
    is_hardcoded_mode = 'hardcoded_test_type' in params and params['hardcoded_test_type'] is not None

    if is_hardcoded_mode:
        params['templates_folder'] = None # No templates folder needed
        emit('system_log', {'message': f'Running in hardcoded mode with test type: "{params["hardcoded_test_type"]}"'})
    
    # Logic for File Upload Mode
    else:
        files = data.get('files', [])
        if not files:
            emit('test_error', {'error': 'Upload Mode is active, but no log template files were provided.'})
            return
        try:
            temp_dir = tempfile.mkdtemp()
            for file_data in files:
                with open(os.path.join(temp_dir, file_data['name']), 'w', encoding='utf-8') as f:
                    f.write(file_data['content'])
            params['templates_folder'] = temp_dir
            emit('system_log', {'message': f'Running in upload mode with {len(files)} template(s).'})
        except Exception as e:
            emit('test_error', {'error': f'Failed to handle uploaded files: {e}'})
            return

    params['generator_cores'] = GENERATOR_CORES
    
    # Use a multiprocessing Queue for safe inter-process communication
    queue = Queue()
    
    # Run the stress test in a separate process to avoid blocking the web server
    test_process = Process(target=run_stress_test, args=(params, queue))
    test_process.start()
    
    # Start a background task within the Flask-SocketIO server to listen to the queue
    socketio.start_background_task(target=queue_emitter, queue=queue)

    emit('test_started', {'message': f'Test started with {params["workers"]} writers and {GENERATOR_CORES} generator cores.'})

@socketio.on('stop_test')
def handle_stop_test():
    """
    Handles the 'stop_test' event. Terminates the running stress test process.
    """
    global test_process
    if test_process and test_process.is_alive():
        test_process.terminate()
        test_process.join() # Clean up the terminated process
        test_process = None
        emit('test_stopped', {'message': 'Test stopped by user.'})
    else:
        emit('test_error', {'error': 'No test is currently running.'})

@socketio.on('test_mdb_connection')
def handle_mdb_test(data):
    """
    Handles the 'test_mdb_connection' event to ping the MongoDB server and report back.
    """
    uri = data.get('mdb_uri')
    
    async def do_test():
        """Asynchronous function to perform the MongoDB connection test."""
        client = None
        try:
            # Use a short timeout to prevent the UI from hanging on a bad connection attempt
            client = motor.motor_asyncio.AsyncIOMotorClient(uri, serverSelectionTimeoutMS=3000)
            await client.admin.command('ping')
            socketio.emit('mdb_connection_result', {'status': 'success', 'message': 'MongoDB connection successful!'})
        except Exception as e:
            socketio.emit('mdb_connection_result', {'status': 'error', 'message': f'Connection failed: {e}'})
        finally:
            if client:
                client.close()
    
    def run_async_wrapper():
        """Synchronous wrapper to correctly run the asyncio coroutine in an eventlet context."""
        try:
            asyncio.run(do_test())
        except RuntimeError: # Handle event loop errors when running inside eventlet
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(do_test())

    socketio.start_background_task(run_async_wrapper)

if __name__ == '__main__':
    # The 'host' parameter makes the server accessible from other devices on your network
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)


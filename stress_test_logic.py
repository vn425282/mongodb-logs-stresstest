import asyncio
import random
import time
import json
import os
import re
from faker import Faker
from collections import Counter
import motor.motor_asyncio
import shutil
import secrets

fake = Faker()

def generate_oid():
    return secrets.token_hex(12)

def generate_trace_id():
    return secrets.token_hex(16)

def generate_span_id():
    return secrets.token_hex(8)

def generate_unix_nano():
    return str(time.time_ns())

PLACEHOLDER_GENERATORS = {
    'timestamp': lambda: fake.date_time_this_month().strftime('%d/%b/%Y:%H:%M:%S %z'),
    'iso_timestamp': fake.iso8601,
    'ip': fake.ipv4,
    'uri': fake.uri_path,
    'referer': fake.uri,
    'user_agent': fake.user_agent,
    'status_code': lambda: random.choice([200, 201, 301, 400, 404, 500]),
    'bytes': lambda: random.randint(100, 15000),
    'login_status': lambda: random.choice(['SUCCESS', 'FAILED']),
    'service_name': lambda: random.choice(['billing-service', 'user-api', 'data-processor']),
    'user_id': lambda: str(fake.uuid4()),
    'username': fake.user_name,
    'exception_type': lambda: random.choice(['NullPointerException', 'IOException', 'ValueError', 'TimeoutException']),
    'file_path': lambda: fake.file_path(depth=3, extension='py'),
    'line_num': lambda: random.randint(10, 200),
    'function_name': fake.word,
    'db_host': lambda: f"db-replica-{random.randint(1,4)}.prod.local",
    'query_duration': lambda: random.randint(5, 200),
    'oid': generate_oid,
    'traceId': generate_trace_id,
    'spanId': generate_span_id,
    'timeUnixNano': generate_unix_nano,
    'observedTimeUnixNano': generate_unix_nano,
    'startTimeUnixNano': generate_unix_nano,
}

def generate_data_from_schema(schema, root_schema=None):
    """
    Recursively generates a Python object (dict/list/primitive) that conforms
    to the given JSON schema.
    """
    root_schema = root_schema or schema
    
    if "$ref" in schema:
        ref_path = schema["$ref"]
        if ref_path.startswith("#/"):
            parts = ref_path[2:].split("/")
            ref_schema = root_schema
            for part in parts:
                ref_schema = ref_schema[part]
            return generate_data_from_schema(ref_schema, root_schema)
        else: # External refs not supported
            return None

    if "oneOf" in schema or "anyOf" in schema:
        sub_schema = random.choice(schema.get("oneOf") or schema.get("anyOf"))
        return generate_data_from_schema(sub_schema, root_schema)

    schema_type = schema.get("type")
    
    if schema_type == "object":
        obj = {}
        if "properties" in schema:
            for key, prop_schema in schema["properties"].items():
                if key in PLACEHOLDER_GENERATORS:
                    obj[key] = PLACEHOLDER_GENERATORS[key]()
                else:
                    obj[key] = generate_data_from_schema(prop_schema, root_schema)
        return obj
    
    elif schema_type == "array":
        num_items = random.randint(1, 3)
        arr = []
        if "items" in schema:
            for _ in range(num_items):
                arr.append(generate_data_from_schema(schema["items"], root_schema))
        return arr

    elif schema_type == "string":
        if schema.get("pattern") == "^[0-9a-fA-F]{24}$": # Specific case for ObjectId hex
            return generate_oid()
        return fake.word()

    elif schema_type == "integer":
        return random.randint(0, 1000)

    elif schema_type == "number":
        return fake.pyfloat(left_digits=3, right_digits=2, positive=True)

    elif schema_type == "boolean":
        return fake.boolean()
        
    return None


def load_templates_from_folder(folder_path):
    """
    Loads templates. If a file is .json, it's parsed. If .txt, it's a string.
    """
    if not os.path.isdir(folder_path): return {}
    templates = {}
    for filename in os.listdir(folder_path):
        filepath = os.path.join(folder_path, filename)
        log_type = os.path.splitext(filename)[0]
        if filename.endswith('.json'):
            try:
                with open(filepath, 'r') as f:
                    templates[log_type] = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse JSON template {filename}: {e}")
        elif filename.endswith('.txt'):
            with open(filepath, 'r') as f:
                templates[log_type] = f.read().strip()
    return templates


def generate_log_from_template(log_template, generators):
    """Generates a log from a simple string template."""
    try:
        data = {key: generator() for key, generator in generators.items()}
        return log_template.format(**data)
    except KeyError:
        return "Log generation failed due to undefined placeholder."

async def push_to_mdb(log_batch, collection):
    if not collection: return
    documents = []
    for log in log_batch:
        if isinstance(log, dict):
            documents.append(log)
        elif isinstance(log, str):
            try:
                documents.append(json.loads(log))
            except json.JSONDecodeError:
                documents.append({"message": log}) # Wrap non-JSON strings
    
    if documents:
        try:
            await collection.insert_many(documents, ordered=False)
        except Exception as e:
            print(f"Error writing to MongoDB: {e}")
            await asyncio.sleep(1)


async def worker(args, stats, log_type_choices, log_templates, mdb_collection):
    while True:
        try:
            log_type = random.choice(log_type_choices)
            template = log_templates[log_type]
            
            log_batch = []
            for _ in range(args['batch_size']):
                if isinstance(template, dict): # It's a parsed JSON Schema
                    generated_log = generate_data_from_schema(template)
                    if generated_log:
                        log_batch.append(generated_log)
                elif isinstance(template, str): # It's a text template
                    generated_log = generate_log_from_template(template, PLACEHOLDER_GENERATORS)
                    log_batch.append(generated_log)

            if log_batch:
                await push_to_mdb(log_batch, mdb_collection)
                stats['total_sent'] += len(log_batch)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Worker error: {e}")
            await asyncio.sleep(1)


async def stats_reporter(stats, queue, duration):
    """Reports stats by putting them into the multiprocessing queue."""
    last_check_time = time.time()
    last_sent_count = 0
    start_time = time.time()
    
    while True:
        await asyncio.sleep(1)
        current_time = time.time()
        current_sent_count = stats['total_sent']
        
        time_delta = current_time - last_check_time
        count_delta = current_sent_count - last_sent_count
        
        rate = count_delta / time_delta if time_delta > 0 else 0
        elapsed = current_time - start_time
        progress = (elapsed / duration) * 100
        
        queue.put({
            'rate': rate, 'total_sent': current_sent_count,
            'elapsed': elapsed, 'progress': min(progress, 100)
        })

        last_check_time = current_time
        last_sent_count = current_sent_count


def parse_distribution_map(distribution_str):
    if not distribution_str: return {}
    try:
        return {k.strip(): int(v) for k, v in (item.split(':') for item in distribution_str.split(','))}
    except Exception: return {}


async def async_main(args, queue):
    """The main async function to run the test."""
    log_templates = load_templates_from_folder(args['templates_folder'])
    if not log_templates:
        queue.put({'error': 'No valid templates were loaded.'})
        return

    distribution_map = parse_distribution_map(args.get('distribution'))
    log_type_choices = []
    if distribution_map:
        for log_type, weight in distribution_map.items():
            if log_type in log_templates:
                log_type_choices.extend([log_type] * weight)
    if not log_type_choices:
        log_type_choices = list(log_templates.keys())

    stats = Counter()
    
    mdb_client = None
    mdb_collection = None
    try:
        mdb_client = motor.motor_asyncio.AsyncIOMotorClient(args['mdb_uri'])
        db = mdb_client[args['mdb_db']]
        mdb_collection = db[args['mdb_collection']]
        await db.command('ismaster')
    except Exception as e:
        queue.put({'error': f"MongoDB connection failed: {e}"})
        if mdb_client: mdb_client.close()
        return

    reporter_task = asyncio.create_task(stats_reporter(stats, queue, args['duration']))
    worker_tasks = [asyncio.create_task(worker(args, stats, log_type_choices, log_templates, mdb_collection)) for _ in range(args['workers'])]

    await asyncio.sleep(args['duration'])

    for task in worker_tasks: task.cancel()
    reporter_task.cancel()
    await asyncio.gather(*worker_tasks, reporter_task, return_exceptions=True)

    if mdb_client:
        mdb_client.close()
    
    queue.put({'final': True, 'total_sent': stats['total_sent'], 'duration': args['duration']})
    queue.put(None) # Sentinel to stop the emitter


def run_stress_test(args, queue):
    """Entry point function to be called by the backend process."""
    try:
        asyncio.run(async_main(args, queue))
    finally:
        if 'templates_folder' in args and os.path.exists(args['templates_folder']):
            shutil.rmtree(args['templates_folder'])
            print(f"Cleaned up temporary directory: {args['templates_folder']}")


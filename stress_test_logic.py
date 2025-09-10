import asyncio
import time
import os
import json
import random
import re
import traceback
from faker import Faker
import motor.motor_asyncio
from bson import ObjectId
from multiprocessing import Pool, Manager

# --- Data Generation Logic (largely unchanged) ---
# This part is kept separate so it can be run in different processes.

FAKER_INSTANCE = Faker()
PLACEHOLDER_GENERATORS = {
    'ip': FAKER_INSTANCE.ipv4, 'user_id': FAKER_INSTANCE.uuid4,
    'timestamp': FAKER_INSTANCE.iso8601, 'iso_timestamp': FAKER_INSTANCE.iso8601,
    'uri': FAKER_INSTANCE.uri, 'status_code': lambda: random.choice([200, 201, 400, 404, 500]),
    'bytes': lambda: random.randint(100, 10000), 'user_agent': FAKER_INSTANCE.user_agent,
    'referer': FAKER_INSTANCE.uri, 'service_name': lambda: random.choice(['auth-service', 'billing-service', 'product-catalog']),
    'exception_type': lambda: random.choice(['NullPointerException', 'IOException']),
    'traceId': lambda: FAKER_INSTANCE.hexify(text='^' * 32), 'spanId': lambda: FAKER_INSTANCE.hexify(text='^' * 16),
    'timeUnixNano': lambda: str(int(time.time() * 1e9)), 'observedTimeUnixNano': lambda: str(int(time.time() * 1e9)),
    'startTimeUnixNano': lambda: str(int(time.time() * 1e9)),
}

# This function will be run in a separate process for CPU-bound generation
def generator_worker(queue, templates_folder, distribution_str, batch_size):
    """A dedicated worker process for generating log data."""
    # Each process gets its own LogGenerator instance
    from log_generator import LogGenerator
    generator = LogGenerator(templates_folder, distribution_str)
    
    if not generator.templates:
        return # Exit if no templates

    while True:
        batch = []
        for _ in range(batch_size):
            _, log_data = generator.generate()
            if log_data is not None:
                batch.append(log_data)
        if batch:
            queue.put(batch)

async def db_writer_worker(queue, mdb_uri, db_name, collection_name):
    """An async worker for writing data from the queue to MongoDB."""
    client = None
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(mdb_uri)
        collection = client[db_name][collection_name]
        while True:
            batch = await queue.get()
            if batch is None:
                break
            if batch:
                await collection.insert_many(batch, ordered=False)
            queue.task_done()
    except Exception:
        print(f"[DB WRITER ERROR] PID {os.getpid()}:\n{traceback.format_exc()}")
    finally:
        if client:
            client.close()

async def main_logic(params, progress_queue):
    """Orchestrates the multi-process generation and async writing."""
    duration = params.get('duration', 60)
    num_db_workers = params.get('workers', 10)
    generator_cores = params.get('generator_cores', 1)

    # Use a Manager queue for safe inter-process communication
    manager = Manager()
    generated_log_queue = manager.Queue(maxsize=generator_cores * 4)
    
    # --- Start CPU-Bound Generator Pool ---
    # This pool will run the generator_worker function across multiple cores
    pool = Pool(processes=generator_cores)
    gen_args = (generated_log_queue, params['templates_folder'], params.get('distribution'), params.get('batch_size', 100))
    for _ in range(generator_cores):
        pool.apply_async(generator_worker, args=gen_args)

    # --- Start IO-Bound Async Database Writers ---
    async_log_queue = asyncio.Queue(maxsize=num_db_workers * 2)
    db_writer_tasks = [
        asyncio.create_task(db_writer_worker(async_log_queue, params['mdb_uri'], params['mdb_db'], params['mdb_collection']))
        for _ in range(num_db_workers)
    ]
    
    start_time = time.time()
    total_sent = 0
    last_update = 0

    # --- Main Bridge Loop ---
    # This loop moves data from the multiprocessing queue to the asyncio queue
    while time.time() - start_time < duration:
        try:
            batch = generated_log_queue.get(timeout=0.1)
            await async_log_queue.put(batch)
            total_sent += len(batch)
        except Exception:
            # Queue is empty, continue loop
            pass

        elapsed = time.time() - start_time
        if elapsed > last_update + 0.5:
            rate = total_sent / elapsed if elapsed > 0 else 0
            progress = (elapsed / duration) * 100
            progress_queue.put({'progress': progress, 'rate': rate, 'total_sent': total_sent, 'elapsed': elapsed})
            last_update = elapsed

    # --- Cleanup ---
    pool.terminate()
    pool.join()
    for _ in range(num_db_workers):
        await async_log_queue.put(None)
    await asyncio.gather(*db_writer_tasks)
    progress_queue.put({'final': True, 'total_sent': total_sent})

def run_stress_test(params, progress_queue):
    """Entry point function."""
    # We need to create a dummy log_generator.py for multiprocessing to work correctly
    # This is a workaround for some OS-specific multiprocessing quirks.
    generator_code = """
import os, json, random, re, time
from faker import Faker
from bson import ObjectId

FAKER_INSTANCE = Faker()
PLACEHOLDER_GENERATORS = {
    'ip': FAKER_INSTANCE.ipv4, 'user_id': FAKER_INSTANCE.uuid4,
    'timestamp': FAKER_INSTANCE.iso8601, 'iso_timestamp': FAKER_INSTANCE.iso8601,
    'uri': FAKER_INSTANCE.uri, 'status_code': lambda: random.choice([200, 201, 400, 404, 500]),
    'bytes': lambda: random.randint(100, 10000), 'user_agent': FAKER_INSTANCE.user_agent,
    'referer': FAKER_INSTANCE.uri, 'service_name': lambda: random.choice(['auth-service', 'billing-service', 'product-catalog']),
    'exception_type': lambda: random.choice(['NullPointerException', 'IOException']),
    'traceId': lambda: FAKER_INSTANCE.hexify('^'*32), 'spanId': lambda: FAKER_INSTANCE.hexify('^'*16),
    'timeUnixNano': lambda: str(int(time.time() * 1e9)), 'observedTimeUnixNano': lambda: str(int(time.time() * 1e9)),
    'startTimeUnixNano': lambda: str(int(time.time() * 1e9)),
}
class LogGenerator:
    def __init__(self, templates_folder, distribution_str=None):
        self.templates = {}
        self.current_schema = {}
        self.load_templates(templates_folder)
        self.distribution = self._parse_distribution(distribution_str)
    def load_templates(self, folder):
        for filename in os.listdir(folder):
            filepath = os.path.join(folder, filename)
            template_name = os.path.splitext(filename)[0]
            try:
                if filename.endswith('.json'):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        if "$schema" in content: self.templates[template_name] = {'type': 'schema', 'content': content}
                        else: self.templates[template_name] = {'type': 'json_template', 'content': json.dumps(content)}
                elif filename.endswith('.txt'):
                     with open(filepath, 'r', encoding='utf-8') as f: self.templates[template_name] = {'type': 'text_template', 'content': f.read()}
            except Exception: continue
    def _parse_distribution(self, dist_str):
        if not dist_str or not self.templates:
            n = len(self.templates); names = list(self.templates.keys()); return (names, [1.0/n]*n) if n > 0 else ([],[])
        names, weights = [], []
        for part in dist_str.split(','):
            if ':' in part:
                name, weight_str = part.split(':', 1); name = name.strip()
                if name in self.templates: names.append(name); weights.append(int(weight_str.strip()))
        if not names: return list(self.templates.keys()), [1.0/len(self.templates)]*len(self.templates)
        total = sum(weights); return names, [w/total for w in weights]
    def _generate_from_schema(self, schema_part):
        if '$ref' in schema_part:
            path = schema_part['$ref'].split('/')[1:]; schema = self.current_schema
            for p in path: schema = schema.get(p, {})
            return self._generate_from_schema(schema)
        t = schema_part.get('type')
        if t == 'object':
            obj = {}
            if 'properties' in schema_part:
                for k, p in schema_part['properties'].items():
                    if k.lower() in ['$oid', '_id']: obj[k] = str(ObjectId())
                    elif 'timeunixnano' in k.lower(): obj[k] = str(int(time.time() * 1e9))
                    elif 'traceid' in k.lower(): obj[k] = FAKER_INSTANCE.hexify('^'*32)
                    elif 'spanid' in k.lower(): obj[k] = FAKER_INSTANCE.hexify('^'*16)
                    else: obj[k] = self._generate_from_schema(p)
            return obj
        elif t == 'array': return [self._generate_from_schema(schema_part['items']) for _ in range(random.randint(1, 3))]
        elif t == 'string': return FAKER_INSTANCE.word()
        elif t == 'integer': return FAKER_INSTANCE.random_int(0, 1000)
        elif t == 'number': return FAKER_INSTANCE.pyfloat()
        elif t == 'boolean': return FAKER_INSTANCE.boolean()
        return None
    def generate(self):
        if not self.distribution[0]: return None, None
        log_type = random.choices(self.distribution[0], self.distribution[1], k=1)[0]
        info = self.templates[log_type]
        if info['type'] == 'schema': self.current_schema = info['content']; return log_type, self._generate_from_schema(info['content'])
        log = info['content']
        for p in re.findall(r'{(\\w+)}', log):
            if p in PLACEHOLDER_GENERATORS: log = log.replace(f'{{{p}}}', str(PLACEHOLDER_GENERATORS[p]()), 1)
        return (log_type, json.loads(log)) if info['type'] == 'json_template' else (log_type, log)

"""
    with open("log_generator.py", "w") as f:
        f.write(generator_code)

    try:
        asyncio.run(main_logic(params, progress_queue))
    finally:
        progress_queue.put(None)
        if os.path.exists("log_generator.py"):
             os.remove("log_generator.py")


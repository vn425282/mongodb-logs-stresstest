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

# --- PREDEFINED LOG TEMPLATES ---

# 1. Simple log template for maximum speed testing
SIMPLE_TEMPLATE = {
    'type': 'json_template',
    'content': json.dumps({
        "message": "Simple high-speed test log", "level": "INFO", "service": "{service_name}",
        "trace_id": "{traceId}", "timestamp": "{iso_timestamp}"
    })
}

# 2. Complex log template based on the original schema
COMPLEX_TEMPLATE = {
    'type': 'schema',
    'content': {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["_id", "resourceMetrics"],
        "properties": {
            "_id": {"$ref": "#/$defs/ObjectId"},
            "resourceMetrics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "resource": {"type": "object"},
                        "schemaUrl": {"type": "string"},
                        "scopeMetrics": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "metrics": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "description": {"type": "string"},
                                                "name": {"type": "string"},
                                                "sum": {
                                                    "type": "object",
                                                    "properties": {
                                                        "aggregationTemporality": {"type": "integer"},
                                                        "dataPoints": {
                                                            "type": "array",
                                                            "items": {
                                                                "type": "object",
                                                                "properties": {
                                                                    "asDouble": {"type": "number"},
                                                                    "attributes": {
                                                                        "type": "array",
                                                                        "items": {
                                                                            "type": "object",
                                                                            "properties": {
                                                                                "key": {"type": "string"},
                                                                                "value": {
                                                                                    "type": "object",
                                                                                    "properties": {"stringValue": {"type": "string"}}
                                                                                }
                                                                            }
                                                                        }
                                                                    },
                                                                    "startTimeUnixNano": {"type": "string"},
                                                                    "timeUnixNano": {"type": "string"}
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                "unit": {"type": "string"}
                                            }
                                        }
                                    },
                                    "scope": {"type": "object"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "$defs": {
            "ObjectId": {
                "type": "object",
                "properties": {"$oid": {"type": "string", "pattern": "^[0-9a-fA-F]{24}$"}}
            }
        }
    }
}


# 3. FOUR (4) NEW LOG TYPES FOR "MIXED" MODE
WIN_SECURITY_LOG = {
    'type': 'schema', 'content': {
        "type": "object", "properties": {
            "timestamp": {"type": "string"}, "hostname": {"type": "string"}, "event_id": {"type": "integer"},
            "source_ip": {"type": "string"}, "user": {"type": "string"}, "status": {"type": "string"}
        }
    }
}
WIN_METRICS_LOG = {
    'type': 'schema', 'content': {
        "type": "object", "properties": {
            "timestamp_nano": {"type": "string"}, "hostname": {"type": "string"}, "metric": {"type": "string"},
            "value": {"type": "number"}, "unit": {"type": "string"}
        }
    }
}
LINUX_METRICS_LOG = {
    'type': 'schema', 'content': {
        "type": "object", "properties": {
            "time": {"type": "string"}, "host": {"type": "string"}, "metric_name": {"type": "string"},
            "value": {"type": "number"}, "tags": {"type": "object"}
        }
    }
}
LINUX_LOG = {
    'type': 'schema', 'content': {
        "type": "object", "properties": {
            "timestamp": {"type": "string"}, "hostname": {"type": "string"}, "process": {"type": "string"},
            "pid": {"type": "integer"}, "message": {"type": "string"}, "severity": {"type": "string"}
        }
    }
}
# ---------------------------------------------

FAKER_INSTANCE = Faker()
PLACEHOLDER_GENERATORS = {
    'ip': FAKER_INSTANCE.ipv4, 'user_id': FAKER_INSTANCE.uuid4, 'timestamp': FAKER_INSTANCE.iso8601,
    'iso_timestamp': FAKER_INSTANCE.iso8601, 'uri': FAKER_INSTANCE.uri,
    'status_code': lambda: random.choice([200, 201, 400, 404, 500]), 'bytes': lambda: random.randint(100, 10000),
    'user_agent': FAKER_INSTANCE.user_agent, 'referer': FAKER_INSTANCE.uri,
    'service_name': lambda: random.choice(['auth-service', 'billing-service', 'product-catalog']),
    'exception_type': lambda: random.choice(['NullPointerException', 'IOException']),
    'traceId': lambda: FAKER_INSTANCE.hexify(text='^' * 32), 'spanId': lambda: FAKER_INSTANCE.hexify(text='^' * 16),
    'timeUnixNano': lambda: str(int(time.time() * 1e9)), 'observedTimeUnixNano': lambda: str(int(time.time() * 1e9)),
    'startTimeUnixNano': lambda: str(int(time.time() * 1e9)),
}

# THIS CODE WILL BE INJECTED INTO THE GENERATOR SUBPROCESSES
GENERATOR_CLASS_CODE = """
import os, json, random, re, time
from faker import Faker
from bson import ObjectId

FAKER_INSTANCE = Faker()
PLACEHOLDER_GENERATORS = {
    'ip': FAKER_INSTANCE.ipv4, 'user_id': FAKER_INSTANCE.uuid4, 'timestamp': FAKER_INSTANCE.iso8601,
    'iso_timestamp': FAKER_INSTANCE.iso8601, 'uri': FAKER_INSTANCE.uri,
    'status_code': lambda: random.choice([200, 201, 400, 404, 500]), 'bytes': lambda: random.randint(100, 10000),
    'user_agent': FAKER_INSTANCE.user_agent, 'referer': FAKER_INSTANCE.uri,
    'service_name': lambda: random.choice(['auth-service', 'billing-service', 'product-catalog']),
    'exception_type': lambda: random.choice(['NullPointerException', 'IOException']),
    'traceId': lambda: FAKER_INSTANCE.hexify('^'*32), 'spanId': lambda: FAKER_INSTANCE.hexify('^'*16),
    'timeUnixNano': lambda: str(int(time.time() * 1e9)), 'observedTimeUnixNano': lambda: str(int(time.time() * 1e9)),
    'startTimeUnixNano': lambda: str(int(time.time() * 1e9)),
}

class LogGenerator:
    def __init__(self, templates_folder, distribution_str=None, hardcoded_test_type=None):
        self.templates = {}
        self.current_schema = {}
        if hardcoded_test_type:
            if hardcoded_test_type == "simple":
                self.templates['simple'] = { 'type': 'json_template', 'content': json.dumps({ "message": "Simple high-speed test log", "level": "INFO", "service": "{service_name}", "trace_id": "{traceId}", "timestamp": "{iso_timestamp}" }) }
                self.distribution = self._parse_distribution("simple:100")
            elif hardcoded_test_type == "complex":
                 # FIXED: The complex dictionary is now properly formatted to avoid syntax errors.
                 self.templates['complex'] = {
                    'type': 'schema',
                    'content': {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "required": ["_id", "resourceMetrics"],
                        "properties": {
                            "_id": {"$ref": "#/$defs/ObjectId"},
                            "resourceMetrics": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "resource": {"type": "object"},
                                        "schemaUrl": {"type": "string"},
                                        "scopeMetrics": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "metrics": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "description": {"type": "string"},
                                                                "name": {"type": "string"},
                                                                "sum": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "aggregationTemporality": {"type": "integer"},
                                                                        "dataPoints": {
                                                                            "type": "array",
                                                                            "items": {
                                                                                "type": "object",
                                                                                "properties": {
                                                                                    "asDouble": {"type": "number"},
                                                                                    "attributes": {
                                                                                        "type": "array",
                                                                                        "items": {
                                                                                            "type": "object",
                                                                                            "properties": {
                                                                                                "key": {"type": "string"},
                                                                                                "value": {
                                                                                                    "type": "object",
                                                                                                    "properties": {"stringValue": {"type": "string"}}
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    },
                                                                                    "startTimeUnixNano": {"type": "string"},
                                                                                    "timeUnixNano": {"type": "string"}
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                "unit": {"type": "string"}
                                                            }
                                                        }
                                                    },
                                                    "scope": {"type": "object"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "$defs": {
                            "ObjectId": {
                                "type": "object",
                                "properties": {"$oid": {"type": "string", "pattern": "^[0-9a-fA-F]{24}$"}}
                            }
                        }
                    }
                }
                 self.distribution = self._parse_distribution("complex:100")
            elif hardcoded_test_type == "mixed":
                self.templates['win_security'] = {'type': 'schema', 'content': {"type": "object", "properties": {"timestamp": {"type": "string"}, "hostname": {"type": "string"}, "event_id": {"type": "integer"}, "source_ip": {"type": "string"}, "user": {"type": "string"}, "status": {"type": "string"}}}}
                self.templates['win_metrics'] = {'type': 'schema', 'content': {"type": "object", "properties": {"timestamp_nano": {"type": "string"}, "hostname": {"type": "string"}, "metric": {"type": "string"}, "value": {"type": "number"}, "unit": {"type": "string"}}}}
                self.templates['linux_metrics'] = {'type': 'schema', 'content': {"type": "object", "properties": {"time": {"type": "string"}, "host": {"type": "string"}, "metric_name": {"type": "string"}, "value": {"type": "number"}, "tags": {"type": "object"}}}}
                self.templates['linux_log'] = {'type': 'schema', 'content': {"type": "object", "properties": {"timestamp": {"type": "string"}, "hostname": {"type": "string"}, "process": {"type": "string"}, "pid": {"type": "integer"}, "message": {"type": "string"}, "severity": {"type": "string"}}}}
                self.distribution = self._parse_distribution("win_security:25,win_metrics:25,linux_metrics:25,linux_log:25")
        else: # Fallback to file upload mode
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
            except Exception: continue # Silently skip malformed files
            
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
                    # Heuristic-based data generation based on key names
                    if k.lower() in ['$oid', '_id']: obj[k] = str(ObjectId())
                    elif 'timeunixnano' in k.lower() or 'timestamp_nano' in k.lower() : obj[k] = str(int(time.time() * 1e9))
                    elif 'traceid' in k.lower(): obj[k] = FAKER_INSTANCE.hexify('^'*32)
                    elif 'spanid' in k.lower(): obj[k] = FAKER_INSTANCE.hexify('^'*16)
                    elif 'timestamp' in k.lower() or 'time' in k.lower(): obj[k] = FAKER_INSTANCE.iso8601()
                    elif 'ip' in k.lower(): obj[k] = FAKER_INSTANCE.ipv4()
                    elif 'hostname' in k.lower() or 'host' in k.lower(): obj[k] = FAKER_INSTANCE.hostname()
                    else: obj[k] = self._generate_from_schema(p)
            return obj
        elif t == 'array': return [self._generate_from_schema(schema_part['items']) for _ in range(random.randint(1, 3))]
        elif t == 'string': return FAKER_INSTANCE.word()
        elif t == 'integer': return FAKER_INSTANCE.random_int(0, 10000)
        elif t == 'number': return FAKER_INSTANCE.pyfloat(left_digits=3, right_digits=2)
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

def init_worker(code_to_exec):
    """Initializer function for each generator process in the pool."""
    exec(code_to_exec, globals())

def generator_worker(queue, templates_folder, distribution_str, batch_size, hardcoded_test_type):
    """The target function for a generator process. It creates logs and puts them in a queue."""
    generator = LogGenerator(templates_folder, distribution_str, hardcoded_test_type)
    if not generator.templates: return
    while True:
        try:
            batch = [log_data for _, log_data in (generator.generate() for _ in range(batch_size)) if log_data is not None]
            if batch: queue.put(batch)
        except Exception:
            pass # Continue generating even if one batch fails

async def db_writer_worker(queue, mdb_uri, db_name, collection_name):
    """Async worker that reads from the queue and writes batches to MongoDB."""
    client = None
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(mdb_uri)
        collection = client[db_name][collection_name]
        while True:
            batch = await queue.get()
            if batch is None: break # End signal
            if batch: await collection.insert_many(batch, ordered=False)
            queue.task_done()
    except Exception:
        print(f"[DB WRITER ERROR] PID {os.getpid()}:\n{traceback.format_exc()}")
    finally:
        if client: client.close()


async def main_logic(params, progress_queue):
    """The main orchestration logic for the stress test."""
    duration = params.get('duration', 60)
    num_db_workers = params.get('workers', 10)
    generator_cores = params.get('generator_cores', 1)
    
    manager = Manager()
    generated_log_queue = manager.Queue(maxsize=generator_cores * 4)
    
    # Create a pool of generator processes
    pool = Pool(processes=generator_cores, initializer=init_worker, initargs=(GENERATOR_CLASS_CODE,))
    
    # Define arguments for the generator workers
    gen_args = (generated_log_queue, params['templates_folder'], params.get('distribution'), params.get('batch_size', 100), params.get('hardcoded_test_type'))
    for _ in range(generator_cores):
        pool.apply_async(generator_worker, args=gen_args)

    # Setup async queue and writer tasks
    async_log_queue = asyncio.Queue(maxsize=num_db_workers * 2)
    db_writer_tasks = [asyncio.create_task(db_writer_worker(async_log_queue, params['mdb_uri'], params['mdb_db'], params['mdb_collection'])) for _ in range(num_db_workers)]
    
    start_time, total_sent, last_update = time.time(), 0, 0
    while time.time() - start_time < duration:
        try:
            # Move logs from the multiprocessing queue to the asyncio queue
            batch = generated_log_queue.get(timeout=0.1)
            await async_log_queue.put(batch)
            total_sent += len(batch)
        except Exception:
            # This is expected if the queue is temporarily empty
            pass
            
        # Send progress updates back to the main process
        elapsed = time.time() - start_time
        if elapsed > last_update + 0.5:
            rate = total_sent / elapsed if elapsed > 0 else 0
            progress = (elapsed / duration) * 100
            progress_queue.put({'progress': progress, 'rate': rate, 'total_sent': total_sent, 'elapsed': elapsed})
            last_update = elapsed
            
    # Clean shutdown
    pool.terminate(); pool.join()
    for _ in range(num_db_workers): await async_log_queue.put(None)
    await asyncio.gather(*db_writer_tasks)
    progress_queue.put({'final': True, 'total_sent': total_sent})


def run_stress_test(params, progress_queue):
    """Entry point for the stress test process."""
    try:
        asyncio.run(main_logic(params, progress_queue))
    finally:
        progress_queue.put(None) # Final signal to the queue emitter


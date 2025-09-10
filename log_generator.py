
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
        for p in re.findall(r'{(\w+)}', log):
            if p in PLACEHOLDER_GENERATORS: log = log.replace(f'{{{p}}}', str(PLACEHOLDER_GENERATORS[p]()), 1)
        return (log_type, json.loads(log)) if info['type'] == 'json_template' else (log_type, log)


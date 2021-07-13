
import os, requests, json, dateparser, time, yaml, boto3
from typing import List
from pathlib import Path
from pprint import pprint

from botocore.client import Config


BATCH_SIZE = 500

wootric_session = requests.Session()
ACCESS_TOKEN = ''
CLIENT_ID = os.getenv('WOOTRIC_CLIENT_ID') 
CLIENT_SECRET = os.getenv('WOOTRIC_CLIENT_SECRET') 
BASE_URL = 'https://api.wootric.com'

stitch_session = requests.Session()
STITCH_CLIENT_ID = os.getenv('STITCH_CLIENT_ID') 
STITCH_TOKEN = os.getenv('STITCH_TOKEN') 
STITCH_BASE_URL = 'https://api.stitchdata.com'
stitch_session.headers = {'Authorization': f'Bearer {STITCH_TOKEN}', 'Content-Type': 'application/json'}

BUCKET = os.getenv('AWS_BUCKET') 

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID_', os.getenv('AWS_ACCESS_KEY_ID')) # lambda doesn't allows this reserve var
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY_', os.getenv('AWS_SECRET_ACCESS_KEY')) # lambda doesn't allow this reserve var
os.environ['AWS_SESSION_TOKEN'] = ''

STATE_KEY = 'wootric.state.json'

s3 = boto3.resource("s3").Bucket(BUCKET)


# init state
state = dict(
  end_users=1420070400,
  responses=1420070400,
  declines=1420070400,
)

def get_access_token():
  global ACCESS_TOKEN
  url = f'{BASE_URL}/oauth/token'
  payload = dict(
    grant_type='client_credentials',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
  )
  resp = wootric_session.post(url, payload)
  data : dict = resp.json()
  ACCESS_TOKEN = data.get('access_token')
  if not ACCESS_TOKEN:
    raise Exception('did not find access_token')
  wootric_session.headers = dict(Authorization=f'Bearer {ACCESS_TOKEN}')



def wootric_request(object_name: str, date_key: str, **params):
  url = f'{BASE_URL}/v1/{object_name}'
  req = requests.models.PreparedRequest()

  params[f"{date_key.replace('_at', '')}[gt]"] = state[object_name]
  page = 0

  all = []
  while True:
    page += 1
    if page > 5: break
    params['page'] = page
    req.prepare_url(url, params)
    print(req.url)
    resp = wootric_session.get(req.url)
    data = resp.json()
    if len(data) == 0:
      break
    all += data

  return all

def send_batch(object_name: str, schema: dict, keys: List[str], records: List[dict]):

  is_datetime = lambda k: schema['properties'].get(k, {}).get('format') == 'date-time'

  messages = []
  for record in records:
    rec = dict(
      action='upsert',
      sequence=int(time.time_ns() / 1000),
      data={},
    )
    for k, v in record.items():
      k = k.replace('.', '_')
      v = (dateparser.parse(str(v))).isoformat() if v and is_datetime(k) else v
      rec['data'][k] = v
    messages.append(rec)

  payload = dict(
    table_name=object_name,
    key_names=keys,
    schema=schema,
    messages=messages,
  )

  # with open('payload.json', 'w') as file:
  #   json.dump(payload, file)

  url = f'{STITCH_BASE_URL}/v2/import/batch'
  resp = stitch_session.post(url, json.dumps(payload))

  data : dict = resp.json()
  print(data)

  status = data.get('status')
  if status != 'OK':
    pprint(dict(status_code=resp.status_code))
    resp.raise_for_status()
  else:
    print(f'pushed {len(records)} records to "{object_name}"')

def load_state():
  global state
  state = json.loads(s3.Object(key=STATE_KEY).get()["Body"].read().decode('utf-8'))

def save_state():
  global state
  s3.Object(key=STATE_KEY).put(Body=json.dumps(state))


def run(event, context):
  global state

  # load wootric access token
  get_access_token()

  # load state
  load_state()

  config_file = Path('config.yaml')
  with config_file.open() as file:
    object_configs = yaml.load(file)

  errors = []
  for object_name, object_config in object_configs.items():
    records : List[dict] = []

    try:
      print(f'Loading {object_name}')
      while True:
        date_key = object_config['date_key']
        params = object_config['params']
        schema = object_config['schema']
        keys = object_config['keys']

        data : List[dict] = wootric_request(object_name, date_key, **params)

        if len(data) == 0:
          if len(records) == 0:
            break
        else:
          records += data

        record = records[-1]
        if date_key in record:
          date_val = dateparser.parse(record[date_key])
          ts_val = int(date_val.timestamp())
          if date_val and ts_val > state[object_name]:
            state[object_name] = ts_val
        else:
          print(f'no datekey: {date_key}')
          pprint(record)

        
        if len(records) >= BATCH_SIZE or len(data) == 0:
          send_batch(object_name, schema, keys, records)

          # save state after successful push
          save_state()
          
          records = []
    except Exception as E:
      errors.append(str(E))
  
  if len(errors) > 0:
    raise Exception('\n\n'.join(errors))
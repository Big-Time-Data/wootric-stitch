
import os, requests, json, dateparser, time, yaml, boto3, slack, random
from typing import List
from pathlib import Path
from pprint import pprint
from traceback import format_exc

from missing_data import get_missing_responses, get_missing_users


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

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID_', os.getenv('AWS_ACCESS_KEY_ID')) # lambda doesn't allow this reserved var
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY_', os.getenv('AWS_SECRET_ACCESS_KEY')) # lambda doesn't allow this reserved var
os.environ['AWS_SESSION_TOKEN'] = '' # lambda provides this reserved var during execution, need to set blank

STATE_KEY = 'wootric.state.json'

s3 = boto3.resource("s3").Bucket(BUCKET)

slack_client = slack.WebhookClient(url=os.getenv('SLACK_WH_TOKEN'))

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


def wootric_response(user_id: str, response_id: str):
  url = f'{BASE_URL}/v1/end_users/{user_id}/responses/{response_id}'
  print(url)
  resp = wootric_session.get(url)
  return resp.json()

def wootric_user(user_id: str):
  url = f'{BASE_URL}/v1/end_users/{user_id}'
  print(url)
  resp = wootric_session.get(url)
  return resp.json()

def wootric_request(object_name: str, date_key: str, **params):
  url = f'{BASE_URL}/v1/{object_name}'
  req = requests.models.PreparedRequest()
  date_val = state[object_name]

  # put random limit because seems some get missed. an attempt to randomize sort anchors
  limit = random.randint(5,29)

  params[f"{date_key.replace('_at', '')}[gte]"] = date_val - 1
  page = 0

  all = []
  while True:
    page += 1
    if page > limit: break
    params['page'] = page
    req.prepare_url(url, params)
    print(req.url)
    resp = wootric_session.get(req.url)
    if resp is None:
      raise Exception(f'Response is for: {req.url}')
    elif not resp.ok:
      raise Exception(f'\n\nHTTP Status Code {resp.status_code} for {req.url}: \n{resp.text}')
    
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

  # re-run for past 3 days, an attempt to fill in any holes
  for k in state:
    state[k] = state.get(k, 1420070400) - 3*24*60*60

def save_state():
  global state
  s3.Object(key=STATE_KEY).put(Body=json.dumps(state))
  print(json.dumps(state))


def run(event, context):
  global state

  try:
    # load wootric access token
    get_access_token()

    # load state
    load_state()
    
  except Exception as E:
    slack_client.send(text=f"Error occurred for Wootric-Stitch Integration:\n{format_exc()}")
    raise E

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

        send_batch(object_name, schema, keys, records)

        record = records[-1]
        if date_key not in record:
          raise Exception(f'no datekey: {date_key}')
        
        records = []

        date_val = dateparser.parse(record[date_key])
        ts_val = int(date_val.timestamp())
        if date_val and ts_val > state[object_name]:
          state[object_name] = ts_val
          save_state()
        else:
          break
        
    except Exception as E:
      errors.append(format_exc())
    finally:
      save_state()

  # Missing users START
  # seems users are missing event with using gte. Gets the IDs from database
  try:
    users = []
    for row in get_missing_users():
      users += [wootric_user(row.end_user_id)]
    
    response_config = object_configs.get('end_users')
    send_batch('end_users', response_config['schema'], response_config['keys'], users)
  except Exception as E:
      errors.append(format_exc())
  # Missing users END

  # Missing responses START
  # seems some responses are missing event with using gte. Gets the IDs from database
  try:
    responses = []
    for row in get_missing_responses():
      responses += [wootric_response(row.user_id, row.last_response__id)]
    
    response_config = object_configs.get('responses')
    send_batch('responses', response_config['schema'], response_config['keys'], responses)
  except Exception as E:
      errors.append(format_exc())
  # Missing responses END

  if len(errors) > 0:
    e = '\n\n'.join(errors)
    slack_client.send(text=f'Error occurred for Wootric-Stitch Integration:\n{e}')
    raise Exception(e)
  
# run(None, None)
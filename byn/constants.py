CASSANDRA_KEYSPACE = 'byn'

CLEAN_NBRB_DATA = 'data/bcse-rates.json'
DXY_12MSK_DARA = 'data/dxy-12MSK.json'
EXTERNAL_RATE_DATA = 'data/forexpf-%s.json'

BACKUP_BUCKET = 'byn-dzmitry-by-backup'
NBRB_BACKUP_PATH = '/tmp/nbrb.backup.csv.gz'

FOREXPF_LONG_POLL_SSE = 'https://charts.profinance.ru/html/tw/sse'
REDIS_CACHE_DB = 1

BCSE_UPDATE_INTERVAL = 15       # seconds
PREDICT_UPDATE_INTERVAL = 1     # seconds
BCSE_LAST_OPERATION_COLOR = '#7cb5ec'
FOREXPF_WORKERS_COUNT = 2
BCSE_USD_REDIS_KEY = 'USD/BYN'
FOREXPF_CURRENCIES_TO_LISTEN = 'EUR', 'RUB', 'UAH', 'DXY'
PREDICT_REDIS_CHANNEL = 'predict'
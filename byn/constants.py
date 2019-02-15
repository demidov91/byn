CASSANDRA_KEYSPACE = 'byn'

CLEAN_NBRB_DATA = 'data/bcse-rates.json'
DXY_12MSK_DARA = 'data/dxy-12MSK.json'
EXTERNAL_RATE_DATA = 'data/forexpf-%s.json'
RIDGE_CACHE_FOLDER = 'data/ridge_cache/'

BACKUP_BUCKET = 'byn-dzmitry-by-backup'

FOREXPF_LONG_POLL_SSE = 'https://charts.profinance.ru/html/tw/sse'
REDIS_CACHE_DB = 1

BCSE_UPDATE_INTERVAL = 15       # seconds
PREDICT_UPDATE_INTERVAL = 5     # seconds
BCSE_LAST_OPERATION_COLOR = '#7cb5ec'
FOREXPF_WORKERS_COUNT = 2
BCSE_USD_REDIS_KEY = 'USD/BYN'
FOREXPF_CURRENCIES_TO_LISTEN = 'EUR', 'RUB', 'UAH', 'DXY'
PUBLISH_PREDICT_REDIS_CHANNEL = 'publish_predict'
FIX_BCSE_TIMESTAMP = 3  # hours

# Standard deviation for USD/BYN exchange rate during a day.
STD_USD_BYN = 0.0016
ENOUGH_FOR_BUILDING_STD = 20
# All model input data is normalized: 1
MAX_PREDICTABLE_DISTANCE = 1

ROLLING_AVERAGE_DURATIONS = (2, 5, 10, 20, 40, 120, 240)

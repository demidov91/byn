
import asyncio

from byn.realtime.external_rates import listen_forexpf
from byn.realtime.bcse import listen_bcse
#from byn.realtime.produce_predict import infinite_prediction
from byn.realtime.api import listen_api

# Initialize logging configuration.
import byn.logging


async def main():
    await asyncio.gather(
        listen_forexpf(),
        listen_bcse(),
#        infinite_prediction(),
        listen_api(),
    )


if __name__ == '__main__':
    asyncio.run(main())

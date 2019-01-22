
import asyncio

from byn.realtime.external_rates import listen_forexpf
from byn.realtime.bcse import listen_bcse
from byn.realtime.produce_predict import start_predicting
from byn.realtime.api import listen_api

# Initialize logging configuration.
import byn.logging


async def main():
    await asyncio.gather(
        listen_forexpf(),
        listen_bcse(),
        start_predicting(),
        listen_api(),
    )


if __name__ == '__main__':
    asyncio.run(main())

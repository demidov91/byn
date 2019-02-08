
import asyncio

from byn.realtime.external_rates import listen_forexpf
from byn.realtime.bcse import listen_bcse
from byn.realtime.synchronization import start as start_synchronization
from byn.realtime.api import listen_api
from byn.realtime.predict_server import run as run_predict_server
from byn.realtime.predict_scheduler import predict_scheduler

# Initialize logging configuration.
import byn.logging


async def main():
    await start_synchronization()

    await asyncio.gather(
        listen_forexpf(),
        listen_bcse(),
        listen_api(),
        run_predict_server(),
        predict_scheduler(),
    )


if __name__ == '__main__':
    asyncio.run(main())

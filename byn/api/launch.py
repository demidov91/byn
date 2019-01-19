
import asyncio

from byn.api.external_rates import listen_forexpf
from byn.api.bcse import listen_bcse

# Initialize logging configuration.
import byn.logging


async def main():
    await asyncio.gather(listen_forexpf(), listen_bcse())


if __name__ == '__main__':
    asyncio.run(main())

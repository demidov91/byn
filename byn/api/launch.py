
import asyncio

from byn.api.external_rates import listen_forexpf

# Initialize logging configuration.
import byn.logging


async def main():
    await asyncio.gather(listen_forexpf())


if __name__ == '__main__':
    asyncio.run(main())

import json
import os
import time

from tda.auth import easy_client, client_from_access_functions
from tda.client import Client
from tda.streaming import StreamClient
import asyncio
import pprint
import tda
import random

API_KEY = "XXXXXX"
ACCOUNT_ID = "XXXXXX"
# If token defined here, use this env, else find token in path. This token will get updated to path.
# MYTOKEN={
#     "access_token": "",
#     "scope": "Hello World!",
#     "expires_in": 1800,
#     "token_type": "Bearer",
#     "expires_at": 1616222008,
#     "refresh_token": ""
# }

## Uncomment to enable websocket debug
import logging
logger = logging.getLogger('websockets.protocol')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

tdlogger = tda.streaming.get_logger()
tdlogger.setLevel(logging.DEBUG)


class MyStreamConsumer:
    """
    We use a class to enforce good code organization practices
    """

    def __init__(self, api_key, account_id, queue_size=1,
                 credentials_path='./ameritrade-credentials.pickle'):
        """
        We're storing the configuration variables within the class for easy
        access later in the code!
        """
        self.api_key = api_key
        self.account_id = account_id
        self.credentials_path = credentials_path
        self.tda_client = None
        self.stream_client = None
        self.symbols = ['QQQ']

        # Create a queue so we can queue up work gathered from the client
        self.queue = asyncio.Queue(queue_size)

    def initialize(self):
        """
        Create the clients and log in. Using easy_client, we can get new creds
        from the user via the web browser if necessary
        """
        def token_read(path):
            def load_refresh_token():
                with open(path, 'r') as tokenfile:
                    token = json.load(tokenfile)
                    return token

            def load_token():
                return MYTOKEN

            if os.path.isfile(self.credentials_path):
                print("returning token file from {}".format(self.credentials_path))
                return load_refresh_token
            else:
                print("returning static token")
                return load_token

        def update_token(path):
            def update_tok(t, refresh_token):
                with open(path, 'w') as f:
                    json.dump(t, f)
            return update_tok


        self.tda_client = client_from_access_functions(api_key=API_KEY,
                                                       token_read_func=token_read(self.credentials_path),
                                                       token_write_func=update_token(self.credentials_path),
                                                       asyncio=False )

        self.stream_client = StreamClient(
            self.tda_client, account_id=self.account_id)

        # The streaming client wants you to add a handler for every service type
        self.stream_client.add_news_headline_handler(
            self.handle_timesale_equity)

    async def stream(self):
        await self.stream_client.login()  # Log into the streaming service
        await self.stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        await self.stream_client.news_headline_subs(self.symbols)

        # Kick off our handle_queue function as an independent coroutine
        asyncio.ensure_future(self.handle_queue())
        # await asyncio.sleep(30)
        print('Starting handle msg loop in stream')
        # Continuously handle inbound messages

        ## Kick off coroutine to add symbols
        asyncio.ensure_future(self.add_me_later())

        while True:
            await self.stream_client.handle_message()


    async def add_me_later(self):
        symbol = [
            'GOOG', 'GOOGL', 'BP', 'CVS', 'ADBE', 'CRM', 'SNAP', 'AMZN',
            'BABA', 'DIS', 'TWTR', 'M', 'USO', 'AAPL', 'NFLX', 'GE', 'TSLA',
            'F', 'SPY', 'FDX', 'UBER', 'ROKU', 'X', 'FB', 'BIDU', 'FIT'
        ]

        while True:
            await asyncio.sleep(random.randint(3,10))
            addme = symbol[random.randint(0, len(symbol) - 1)]

            await self.stream_client.news_headline_subs([addme])


    async def handle_timesale_equity(self, msg):
        """
        This is where we take msgs from the streaming client and put them on a
        queue for later consumption. We use a queue to prevent us from wasting
        resources processing old data, and falling behind.
        """
        # if the queue is full, make room
        if self.queue.full():
            await self.queue.get()
        await self.queue.put(msg)

    async def handle_queue(self):
        """
        Here we pull messages off the queue and process them.
        """
        while True:
            msg = await self.queue.get()
            pprint.pprint(msg)


async def main():
    """
    Create and instantiate the consumer, and start the stream
    """
    consumer = MyStreamConsumer(API_KEY, ACCOUNT_ID)
    consumer.initialize()
    await consumer.stream()

if __name__ == '__main__':
    asyncio.run(main())

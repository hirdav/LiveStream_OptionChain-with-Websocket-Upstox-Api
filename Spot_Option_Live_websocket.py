import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict

import MarketDataFeed_pb2 as pb

# Define a list to store received data
received_data = []

def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(
        upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""

    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Configure OAuth2 access token for authorization
    configuration = upstox_client.Configuration()

    api_version = '2.0'
    configuration.access_token = access_token = 'eyJ0xxxxxxxY2UiLCJleHAiOjE3MjYxxxLkV3p2gpTAo63X3f1t9HuqA4'

    # Get market data feed authorization
    response = get_market_data_feed_authorize(
        api_version, configuration)

    # Connect to the WebSocket with SSL context
    async with websockets.connect(response.data.authorized_redirect_uri, ssl=ssl_context) as websocket:
        print('Connection established []')

        await asyncio.sleep(0)  # Wait for 1 second

        # Data to be sent over the WebSocket example "NSE_INDEX|Nifty Bank","NSE_FO|35029", "NSE_INDEX|Nifty 50", "NSE_EQ|INE010B01027", "NSE_FO|54299"
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": ["NSE_INDEX|Nifty 50"]
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        print('Subscription request sent')  # Add this line

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)

            # Append the dictionary to the list
            received_data.append(data_dict)

            print('Received data:', data_dict)  # Add this line


# Run the fetch_market_data function in an asyncio event loop
asyncio.run(fetch_market_data())





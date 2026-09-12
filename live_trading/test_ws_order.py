import asyncio
import websockets
import json
import hmac
import hashlib
import time
import os
from dotenv import load_dotenv

load_dotenv("keys.env")

API_KEY = os.environ.get('BINANCE_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET')

def sign(query):
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

async def test_order():
    url = "wss://ws-fapi.binance.com/ws-fapi/v1"
    async with websockets.connect(url) as ws:
        ts = int(time.time() * 1000)
        # Test a small BUY limit order far below market price
        query = f"apiKey={API_KEY}&newClientOrderId=test1234&price=0.100000&quantity=30.00000000&reduceOnly=false&side=BUY&symbol=LSKUSDT&timeInForce=FOK&timestamp={ts}&type=LIMIT"
        sig = sign(query)
        
        params = {
            "apiKey": API_KEY,
            "newClientOrderId": "test1234",
            "price": "0.100000",
            "quantity": "30.00000000",
            "reduceOnly": "false",
            "side": "BUY",
            "symbol": "LSKUSDT",
            "timeInForce": "FOK",
            "timestamp": ts,
            "type": "LIMIT",
            "signature": sig
        }
        
        payload = {
            "id": "123",
            "method": "order.place",
            "params": params
        }
        
        print("Sending payload 1 (reduceOnly=false string)")
        await ws.send(json.dumps(payload))
        resp = await ws.recv()
        print("Response 1:", resp)

        ts = int(time.time() * 1000)
        query2 = f"apiKey={API_KEY}&newClientOrderId=test12345&price=0.100000&quantity=30.00000000&reduceOnly=true&side=BUY&symbol=LSKUSDT&timeInForce=FOK&timestamp={ts}&type=LIMIT"
        sig2 = sign(query2)
        params2 = {
            "apiKey": API_KEY,
            "newClientOrderId": "test12345",
            "price": "0.100000",
            "quantity": "30.00000000",
            "reduceOnly": "true",
            "side": "BUY",
            "symbol": "LSKUSDT",
            "timeInForce": "FOK",
            "timestamp": ts,
            "type": "LIMIT",
            "signature": sig2
        }
        payload2 = {
            "id": "124",
            "method": "order.place",
            "params": params2
        }
        print("Sending payload 2 (reduceOnly=true string)")
        await ws.send(json.dumps(payload2))
        resp2 = await ws.recv()
        print("Response 2:", resp2)


if __name__ == "__main__":
    asyncio.run(test_order())

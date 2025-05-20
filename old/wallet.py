import os
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
from dotenv import load_dotenv
import json

load_dotenv()

# API_KEY = os.getenv("HL_API_KEY")
# SECRET_KEY = os.getenv("HL_SECRET_KEY")
ADDRESS = os.getenv('METAMASK_ADDRESS')

# class Wallet:
#     def __init__(self):
#         self.api = Info(base_url=MAINNET_API_URL)

#     def get_wallet_assets(self):
#         """
#         Retrieve all assets in the wallet.
#         """
#         # Fetch all assets
#         response = self.api.all_mids()
#         return response


info = Info(MAINNET_API_URL)

user_state = info.user_state(ADDRESS)
positions = []
for position in user_state["assetPositions"]:
    positions.append(position["position"])
if len(positions) > 0:
    print("positions:")
    for position in positions:
        print(json.dumps(position, indent=2))
else:
    print("no open positions")
import os
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL

API_KEY = os.getenv("HL_API_KEY")
SECRET_KEY = os.getenv("HL_SECRET_KEY")

class Wallet:
    def __init__(self):
        self.api = Info(base_url=MAINNET_API_URL)

    def get_wallet_assets(self):
        """
        Retrieve all assets in the wallet.
        """
        response = self.api.post("/info", {"type": "wallet"})
        return response

wallet = Wallet()
assets = wallet.get_wallet_assets()

print(assets)

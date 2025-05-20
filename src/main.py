import source
from strategy.volume_collector import VolumeCollector
import asyncio

vc = VolumeCollector(source.Hyperliquid('@151'))
asyncio.run(vc.flush())
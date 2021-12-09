import sys
import os
sys.path.append(os.path.abspath("/opt/aqueduct/aqueduct/services"))
from food_supply_chain_service import *
import time
import logging

while True:
    logging.info("Checking for work")
    FoodSupplyChainService.pop_and_do_work()
    time.sleep(5)

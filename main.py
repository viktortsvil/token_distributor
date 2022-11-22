import sys
import logging
from datetime import datetime
from distributor.modules.base import main

logging.basicConfig(filename=f"distributor/log/{str(datetime.utcnow()).split('.')[0]}.txt", level=logging.DEBUG)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        logging.critical("Not enough arguments in sys.argv. Exiting")
    else:
        logging.info(f"PIPELINE NAME: {sys.argv[1]}\n--------------------------------")
        main(sys.argv[1])
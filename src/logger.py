import sys
import logging

logging.basicConfig(format='%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s',
                    level='INFO',
                    handlers=[logging.StreamHandler(sys.stdout)])

logger = logging.getLogger(__name__)

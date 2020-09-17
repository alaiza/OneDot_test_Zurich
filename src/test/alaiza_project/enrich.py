import logging
import mysql.connector

from sqlalchemy import create_engine


_logger = logging.getLogger(__name__)

class Enrich:

    def __init__(self, param):
        self.__host = param


    def run(self):
        _logger.info('here Ill run all the logic')
        return 'done'


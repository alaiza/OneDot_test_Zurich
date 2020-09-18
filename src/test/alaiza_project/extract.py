import logging
import mysql.connector

from sqlalchemy import create_engine


_logger = logging.getLogger(__name__)

class Extract:

    def __init__(self, Sparksession, preprocesseddataframe):
        self.__Sparksession = Sparksession
        self.__preprocesseddataframe = preprocesseddataframe

    def run(self):
        _logger.info('here Ill run all the logic')
        return 'done'


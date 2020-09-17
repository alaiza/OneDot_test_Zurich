import logging
import mysql.connector



_logger = logging.getLogger(__name__)

class Preprocess:

    def __init__(self, Sparksession, rawdataframe):
        self.__Sparksession = Sparksession
        self.__rawdataframe = rawdataframe


    def run(self):
        _logger.info('here Ill run all the logic')
        return 'done'




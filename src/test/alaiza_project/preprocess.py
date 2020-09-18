import logging
import mysql.connector
from pyspark.sql.functions import first


_logger = logging.getLogger(__name__)

class Preprocess:

    def __init__(self, Sparksession, rawdataframe):
        self.__Sparksession = Sparksession
        self.__rawdataframe = rawdataframe


    def run(self):
        _logger.info('here Ill run all the logic')
        df2 = self.__rawdataframe.drop('entity_id') # for make it unique

        df3 = df2.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull").pivot(
            "Attribute Names").agg(first("Attribute values")) #Pivot dataframe
        return df3




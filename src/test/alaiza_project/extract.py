import logging

_logger = logging.getLogger(__name__)

class Extract:

    def __init__(self, Sparksession, preprocesseddataframe):
        self.__Sparksession = Sparksession
        self.__preprocesseddataframe = preprocesseddataframe

    def run(self):
        self.__preprocesseddataframe.registerTempTable("supplier_car_normalized")
        df_extracted = self.__Sparksession.sql(
            """
            select 
            *, 
            split(ConsumptionTotalText,' ')[0] as `extracted-value-ConsumptionTotalText`,
            split(ConsumptionTotalText,' ')[1] as `extracted-unit-ConsumptionTotalText` 
            from supplier_car_normalized
            """
        )
        return df_extracted


import logging
import mysql.connector

from sqlalchemy import create_engine


_logger = logging.getLogger(__name__)

class Integrate:

    def __init__(self, Sparksession, preprocesseddataframe):
        self.__Sparksession = Sparksession
        self.__preprocesseddataframe = preprocesseddataframe

    def run(self):
        self.__preprocesseddataframe.registerTempTable("supplier_car_extracted")
        df_integrated = self.__Sparksession.sql(
            """
            select 
            BodyTypeText as carType,
            Color as color,
            ConditionTypeText as Condition,
            'N/A' as currency,
            'N/A' as drive,'
            City as city,
            City+'- country'as country,
            Maker as as Make,
            FirstRegYear as manufacture_year,
            CASE
                WHEN Co2EmissionText is null THEN null
                WHEN split(Co2EmissionText,'/')[1] = 'km' THEN 'kilometer'
                else 'mile' 
            end as mileage_unit
            from supplier_car_extracted
            """
        )
        return df_integrated


import logging
import mysql.connector


_logger = logging.getLogger(__name__)

class Normalize:

    def __init__(self, Sparksession, preprocesseddataframe):
        self.__Sparksession = Sparksession
        self.__preprocesseddataframe = preprocesseddataframe


    def run(self):
        _logger.info('auxiliar dimensional table for color')
        df_color_aux = self.__Sparksession.createDataFrame(
            [
                ('bordeaux', 'red'),  # create your data here, be consistent in the types.
                ('grün', 'grey'),
                ('schwarz', 'black'),
                ('grau', 'grey'),
                ('braun', 'brown'),
                ('weiss', 'white'),
                ('blau', 'blue'),
                ('beige', 'beige'),
                ('silber', 'silver'),
                ('anthrazit', 'anthracite'),
                ('rot', 'red'),
                ('mehrfarbig', 'multicolored')
            ],
            ['rawcolor', 'englishcolor']  # add your columns label here
        )
        df_color_aux.registerTempTable("color_aux_dimension")

        df_brand_aux = self.__Sparksession.createDataFrame(
            [
                ('LAMBORGHINI', 'Lamborghini'),  # create your data here, be consistent in the types.
                ('PORSCHE', 'Porsche'),
                ('HYUNDAI', 'Hyundai'),
                ('NSU', 'NSU'),
                ('FIAT', 'Fiat'),
                ('DATSUN', 'Datsun'),
                ('WIESMANN', 'Wiesmann'),
                ('TOYOTA', 'Toyota'),
                ('SUBARU', 'Subaru'),
                ('NISSAN', 'Nissan'),
                ('BMW-ALPINA', 'BMW'),
                ('CITROEN', 'Citroen'),
                ('BENTLEY', 'Bentley'),
                ('MATRA', 'Matra'),
                ('AUDI', 'Audi'),
                ('FORD', 'Ford'),
                ('AUTOBIANCHI', 'Autobianchi'),
                ('SEAT', 'Seat'),
                ('FERRARI', 'Ferrari'),
                ('MINI', 'Mini')
            ],
            ['rawmake', 'brand']  # add your columns label here
        )
        df_brand_aux.registerTempTable("brand_aux_dimension")

        self.__preprocesseddataframe.registerTempTable("preprocessed_supplier_car")

        #perform the updates if there are matches between aux dimensional table and facts table
        df_normalized = self.__Sparksession.sql("""
                    select 
                    ID, 
                    coalesce(Z.brand, MakeText, null) as Maker,
                    ModelText, 
                    ModelTypeText, 
                    TypeName, 
                    TypeNameFull, 
                    BodyColorText, 
                    BodyTypeText, 
                    Ccm, 
                    City, 
                    Co2EmissionText, 
                    ConditionTypeText, 
                    ConsumptionRatingText, 
                    ConsumptionTotalText, 
                    Doors, 
                    DriveTypeText, 
                    FirstRegMonth,
                    FirstRegYear,
                    FuelTypeText, 
                    Hp,
                    coalesce(Y.englishcolor,InteriorColorText, null) as Color, 
                    Km,
                    Properties,
                    Seats,
                    TransmissionTypeText 
                    from preprocessed_supplier_car X
                    left join color_aux_dimension Y
                    on X.InteriorColorText = Y.rawcolor
                    left join brand_aux_dimension Z
                    on X.MakeText = Z.brand
                    """)
        return df_normalized


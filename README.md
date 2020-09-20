# zurich_test_project
This is a test to check my performance for ONE DOT.



1 - Code
===============



<h3> PROJECT </h3>

<p>You can execute the code executing the launcher.py with the parameters</p>

1. step: choices=[1,2,3,4,5], Every number means one of the steps possiblke(1- preprocess, 2- Normalize, 3- Extract, 4- Integrate, 5- Enrich)
2. file: name of the file contained in the folder "input", for the exercise "supplier_car.json"


To execute the code you need to perform this command:

```>> spark-submit --py-files=src.zip --files=configuration launcher.py --step 5 --file gs://crypto-alaiza-project/manual_file_onedot/supplier_car.json```


2 - Logic
===============

The project is composed by 5 stages defined as objects with a run method that will contain all the executable logic

Once you select the step that you want to execute, the process will start executing all the needed stages till it 
finishes in the one selected, it will notify at the end of the execution where and what expect of the execution.

3 - The steps
===============

1. Preprocessing: Erase unique identifier and melts the dataframe generating new columns
2. Normalize: generate two dimensional table and with coalesce and left join modifies the values
3. Extract: from the column ConsumptionTotalText with command split generates two different columns
4. Integrate: Generates the final dataframe selecting the needed fields and making final modifications
5. Enrich: here we could generate metrics as rates between two other metrics or categorizations using other parameters

4 - Problems Found
===============
Due to the lack of time to integrate this solution I forgot to take care with encodings and in case of values out off the basic ascii encoding, python2.7 is not able to manage them
without converting them to unicode and decoding them carefully, I know it is a problem on the current languaje but migrating it to python3 would be an easy solution.

I also found several fields in the "integrate" step that I couldnt find easily and I forced them to becoma "NA" values.

5 - Reasons for this type of development
===============
I preferred to develop an entire project instead of a jupyter notebook or a simple script just for showing a more clear an maintenable structure for a data engineering team.

Future aproximations or upgrades would be to add a cdi logic to compress automatically the src and just deploy in buckets the needed parts of this code.

6 - Basic code sequence
===============

> pip install pandas

> pip install xlwt

```
from pyspark.sql.functions import first


nameinputfile= 'gs://crypto-alaiza-project/manual_file_onedot/supplier_car.json'

spark = SparkSession.builder.appName('pyspark-Zurich_Test').getOrCreate()

df = spark.read.json(nameinputfile)

df2 = df.drop('entity_id')

dfpreprocessed = df2.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull").pivot(
            "Attribute Names").agg(first("Attribute values"))

text = 'preprocessed'
dfpreprocessed.toPandas().to_excel('{0}-target_data_custom.xls'.format(text), sheet_name = 'Sheet1', index = False)



df_color_aux = spark.createDataFrame(
[
('bordeaux', 'red'),
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
['rawcolor', 'englishcolor']
)

df_color_aux.registerTempTable("color_aux_dimension")

df_brand_aux = spark.createDataFrame(
[
('LAMBORGHINI', 'Lamborghini'),
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
['rawmake', 'brand'] 
)

df_brand_aux.registerTempTable("brand_aux_dimension")

df_modified.registerTempTable("preprocessed_supplier_car")

df_normalized = spark.sql("""
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


text = 'normalized'
df_normalized.toPandas().to_excel('{0}-target_data_custom.xls'.format(text), sheet_name = 'Sheet1', index = False)



df_modified.registerTempTable("supplier_car_normalized")

df_extracted = spark.sql(
            """
            select 
            *, 
            split(ConsumptionTotalText,' ')[0] as `extracted-value-ConsumptionTotalText`,
            split(ConsumptionTotalText,' ')[1] as `extracted-unit-ConsumptionTotalText` 
            from supplier_car_normalized
            """
        )

text = 'extracted'
df_extracted.toPandas().to_excel('{0}-target_data_custom.xls'.format(text), sheet_name = 'Sheet1', index = False)



df_integrated = spark.sql(
            """
            select 
            BodyTypeText as carType,
            Color as color,
            ConditionTypeText as Condition,
            'N/A' as currency,
            'N/A' as drive,
            City as city,
            City+'- country'as country,
            Maker as Make,
            FirstRegYear as manufacture_year,
            Km as Mileage,
            CASE
                WHEN Co2EmissionText is null THEN 'null'
                WHEN split(Co2EmissionText,'\/')[1] == 'km' THEN 'kilometer'
                ELSE 'mile' 
            END AS mileage_unit,
            ModelText as model,
            ModelTypeText as model_variant,
            'false' as prince_on_request,
            'car' as type,
            'null' as zip,
            CASE
                WHEN `extracted-unit-ConsumptionTotalText` == 'l/100km' THEN 'l_km_consumption'
                ELSE 'null'
            end as fuel_consumption_unit
            from supplier_car_extracted
            """
            )

text = 'integrated'
df_integrated.toPandas().to_excel('{0}-target_data_custom.xls'.format(text), sheet_name = 'Sheet1', index = False)

```
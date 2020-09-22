import yaml
import src.test.alaiza_project.manager as manager
import time
from pyspark.sql import SparkSession
from src.test.alaiza_project.preprocess import Preprocess
from src.test.alaiza_project.normalize import Normalize
from src.test.alaiza_project.extract import Extract
from src.test.alaiza_project.integrate import Integrate
from src.test.alaiza_project.enrich import Enrich



def main_zurich(arguments, logger):
    try:

        start_time = time.time()

        ##########PARAMETERIZED
        step = int(arguments.get('step'))
        step = 5
        nameinputfile = arguments.get('file')
        nameinputfile= 'gs://crypto-alaiza-project/manual_file_onedot/supplier_car.json'


        ##########CONFIG_SPARKSESSION
        #thriftname = 'thrift://' + socket.gethostname() + ':9083'
        #SparkContext.setSystemProperty('hive.metastore.uris', thriftname)
        #sparkSession = (
            #SparkSession.builder.appName('pyspark-Zurich_Test').enableHiveSupport().getOrCreate())

        spark = SparkSession.builder.appName('pyspark-Zurich_Test').getOrCreate()
        df = manager.getdfFile(spark,nameinputfile)

        ##########START_EXECUTION
        counter_step_to_execute = 1

        if counter_step_to_execute <= step:
            preprocess = Preprocess(spark,df)
            df_modified = preprocess.run()
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            normalize = Normalize(spark,df_modified)
            df_modified = normalize.run()
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            extract = Extract(spark,df_modified)
            df_modified = extract.run()
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            integrate = Integrate(spark,df_modified)
            df_modified = integrate.run()
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            enrich = Enrich(spark,df_modified)
            df_modified = enrich.run()
        #### Export the solution

        manager.exportToExcel(df_modified, step-1)


    except:
        logger.critical("something went really bad")
    finally:
        print("--- %s seconds ---" % (time.time() - start_time))



def load_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


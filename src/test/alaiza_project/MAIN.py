from timeit import default_timer as timer
from src.test.alaiza_project.database_service import DBService
import yaml
import src.test.alaiza_project.manager as manager
import time
import time
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from src.test.alaiza_project.preprocess import Preprocess
from src.test.alaiza_project.normalize import Normalize
from src.test.alaiza_project.extract import Extract
from src.test.alaiza_project.integrate import Integrate
from src.test.alaiza_project.enrich import Enrich
import socket

import sys


def main_zurich(arguments, logger):
    try:

        start_time = time.time()

        ##########PARAMETERIZED
        step = arguments.get('step')
        nameinputfile = arguments.get('file')


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
            Preprocess(spark,df)
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            Normalize(sparkSession)
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            Extract(sparkSession)
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            Integrate(sparkSession)
            counter_step_to_execute = counter_step_to_execute + 1
        if counter_step_to_execute <= step:
            Enrich(sparkSession)

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


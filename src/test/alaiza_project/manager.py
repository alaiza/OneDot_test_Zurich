import logging
import pandas
import subprocess
import csv
import os
import time


_logger = logging.getLogger(__name__)



def getdfFile(spark,nameinputfile):
    try:
        df = spark.read.json(nameinputfile)
        return df
    except:
        _logger.critical('Something didnt worked as expected while reading file')


def exportToExcel(df, step):
    try:
        if step == 1:
            text = 'preprocess'
        elif step == 2:
            text = 'normalize'
        elif step == 3:
            text = 'extract'
        elif step == 4:
            text = 'integrate'
        elif step == 5:
            text = 'enrich'
        else:
            text = 'unknown'
        df.toPandas().to_excel('{0}-target_data_custom.xls'.format(text), sheet_name = 'Sheet1', index = False)
    except:
        _logger.critical('Something didnt worked as expected while reading file')


def exportToCSV(rows,costtype,dtnow_string):
    fp = open("""./output/csv_export_{1}_{0}.csv""".format(dtnow_string,costtype), 'w')
    myFile = csv.writer(fp)
    myFile.writerows(rows)
    fp.close()



def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err



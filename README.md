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


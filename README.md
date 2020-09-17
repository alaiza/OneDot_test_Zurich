# zurich_test_project
This is a test to check my performance for ONE DOT.



1 Code
===============



<h3> PROJECT </h3>

<p>You can execute the code executing the launcher.py with the parameters</p>

1. step: choices=[1,2,3,4,5], Every number means one of the steps possiblke(1- preprocess, 2- Normalize, 3- Extract, 4- Integrate, 5- Enrich)
2. file: name of the file contained in the folder "input", for the exercise "supplier_car.json"


To execute the code you need to perform this command:

```>> spark-submit --py-files=src.zip --files=configuration,incoming,outcoming launcher.py --step 5 --file supplier_car.json```


2 Logic
===============

The project is composed by 5 stages defined as objects with a run method that will contain all the executable logic

Once you select the step that you want to execute, the process will start executing all the needed stages till it 
finishes in the one selected, it will notify at the end of the execution where and what expect of the execution.


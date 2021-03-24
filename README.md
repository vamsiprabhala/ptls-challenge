#### Instructions to run the app

2 options are provided to run the app which are explained below.

#### Option1 - Makefile

Download source code to a local directory using 

    $ git clone git@github.com:vamsiprabhala/ptls-challenge.git`

Then 

    $ cd ptls-challenge
    $ make run JSON_FILE_PATH=ABSOLUTE_FILE_SYSTEM_PATH_WITH_FILE_NAME

The following steps will occur on executing `make run`
 - virtual environment will be created
 - required packages will be installed
 - app.py will be run and results are printed to `stdout`

To clean up after the run, use 

    $ make clean


#### Option2 - Manual

##### Download source code
Download source code to a local directory using 

    git clone git@github.com:vamsiprabhala/ptls-challenge.git

##### Setup virtual environment
    python -m venv .venv 

##### Activate virtual environment
    source .venv/bin/activate

##### Install required packages
    python -m pip install -r requirements.txt

##### Run the app
Execute the following from the top level directory `ptls_challenge`

    python ptls_challenge/app.py JSON_FILE_NAME

Provide the input json file path argument to the script. By default, it computes all the metrics 
and prints the results to stdout.
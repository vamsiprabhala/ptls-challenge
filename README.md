#### ptls-challenge

##### Download source code
Download source code to a local directory using 

`git clone git@github.com:vamsiprabhala/ptls-challenge.git` 

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
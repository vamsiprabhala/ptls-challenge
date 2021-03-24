#### Data Analysis
Data analysis for the questions was done in 2 ways.
1. Using PySpark to compute the metrics
2. Python code to replicate the metric computation

##### Python Code programming model
The code was implemented keeping in mind the constraint that file won't fit in memory and that the data can be big. 
MapReduce programming model was chosen to implement the solution with the following structure. On a high level, the program has the following steps. 

1. Split the file into multiple smaller chunks by choosing a chunk size = 100,000 json records each. Python package `ijson` was used to parse the json file. An example of parsed input is shown below. Parsed output helps keep track of the start and end of json record.  

    start_array None
    item start_map None
    item map_key messageSequenceNumber
    item.messageSequenceNumber number 255372
    item map_key traceId
    item.traceId start_map None
    item.traceId map_key mostSignificantBits
    item.traceId.mostSignificantBits number 7012058579801421000
    item.traceId map_key leastSignificantBits
    item.traceId.leastSignificantBits number 8736631216184186000
    ....
    item end_map None
    ....
    end_array None

2. A base MapReducer class which reads the split files and defines methods `mapper`, `reducer` and `compute` with no implementation defined.

3. All the metrics inherit from the base MapReducer class and provide implementations for `mapper`, `reducer` and `compute`. 
   - `mapper` yields a generator object with the required data
   - `reducer` reads the `mapper` output and computes a given metric
   - `compute` is defined as a higher order function which passes the `mapper` output as `reducer` input 


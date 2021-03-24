import os 
import glob
from typing import List

class MapReducer:
    """
    Abstract class 
    Implementation of mapper, reducer and compute methods 
    should be defined by child classes which calculate a given metric
    """

    def __init__(self, split_files: List[str]):
        self.split_files = split_files


    def read_split_files(self):
        if len(self.split_files) == 0:
            logging.info("======Received 0 Split files. Nothing to process======")
        for filename in self.split_files:
            with open(filename, 'r') as f:
                for line in f:
                    if len(line.split()) == 3:
                        yield line.split()

    
    def mapper(self):
        raise NotImplementedError


    def reducer(self, map_output):
        raise NotImplementedError


    def compute(self):
        raise NotImplementedError


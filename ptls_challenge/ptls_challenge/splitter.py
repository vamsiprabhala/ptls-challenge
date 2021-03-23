import os
import sys
import ijson
from ptls_challenge import helpers
import logging

class Splitter:
    def __init__(self, file_path: str, file_to_split: str):
        self.file_path = file_path 
        self.file_to_split = file_to_split
        self.logging_config = helpers.logging_config()

    def splitFile(self, record_count: int=100000) -> None:
        """
        Take an input json file and split it into multiple files
        of record_count json records per file
        :params
        record_count -> default set to 100K json records
        """
        file_to_parse = os.path.join(self.file_path,self.file_to_split)
        logging.info("Splitting file {}".format(file_to_parse))
        
        #File existence check
        if not os.path.isfile(file_to_parse):
            raise FileNotFoundError(file_to_parse)

        #File format check
        file_name, file_extension = os.path.splitext(file_to_parse)
        if file_extension != '.json':
            logging.error("Not a valid file format. Pass in a JSON file to process. Exiting.")
            sys.exit(-1)

        #Process file using ijson package
        #Read in one line of file at a time 
        with open(file_to_parse,'r') as json_file:
            records = 0
            split_num = 0
            out_file = os.path.join(self.file_path, 'split-{}.txt'.format(split_num))
            outfile = open(out_file,'w')
            logging.info("Writing split = {}".format(split_num))
            for prefix,event,value in ijson.parse(json_file):
                outfile.write("{} {} {}\n".format(prefix,event,value))
                if (prefix, event, value) == ('item', 'start_map', None):
                    records += 1
                if (prefix, event, value) == ('item', 'end_map', None) and record_count == records:
                    outfile.close()
                    logging.info("Writing split = {} complete".format(split_num))
                    logging.info("="*20)
                    split_num += 1
                    records = 0
                    out_file = out_file = os.path.join(self.file_path, 'split-{}.txt'.format(split_num))
                    outfile = open(out_file,'w')
                    logging.info("Writing split = {}".format(split_num))
        logging.info("File split complete. Wrote {} splits".format(split_num+1))

    
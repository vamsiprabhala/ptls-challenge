import os
import sys
import ijson
from src import logger
import logging
from typing import List

class Splitter:
    def __init__(self, file_path: str):
        self.file_path = file_path 

    def splitFile(self, record_count: int=100000) -> List[str]:
        """
        Take an input json file and split it into multiple files
        of record_count json records per file
        :params
        record_count -> default set to 100K json records
        :output
        split_files -> List of split files
        """
        logger.logging_config()
        file_to_parse = self.file_path
        split_files = []
        logging.info("Splitting file {}".format(file_to_parse))
        
        #File existence check
        if not os.path.isfile(file_to_parse):
            raise FileNotFoundError(file_to_parse)

        #File format check
        file_path, file_name = os.path.split(file_to_parse)
        if not file_name.endswith('.json'):
            logging.error("Not a valid file format. Pass in a JSON file to process. Exiting.")
            sys.exit(-1)

        #Process file using ijson package
        #Read in one line of file at a time 
        with open(file_to_parse,'r') as json_file:
            records = 0
            split_num = 0
            out_file = os.path.join(file_path, 'split-{}.txt'.format(split_num))
            split_files.append(out_file)
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
                    out_file = os.path.join(file_path, 'split-{}.txt'.format(split_num))
                    split_files.append(out_file)
                    outfile = open(out_file,'w')
                    logging.info("Writing split = {}".format(split_num))
        logging.info("File split complete. Wrote {} splits".format(split_num+1))
        return split_files

    
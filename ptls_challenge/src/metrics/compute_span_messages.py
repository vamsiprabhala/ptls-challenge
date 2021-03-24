from src.base.base_map_reducer import MapReducer
from typing import List
import logging

class ComputeSpanMessages(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        yield a tuple of (messageSequenceNumber,1)
        """
        logging.info("=====Start Mapper Step=====")
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.messageSequenceNumber', 'number'):
                yield (value,1)
        logging.info("=====Mapper Step Complete=====")

    def reducer(self,map_output) -> int:
        """
        add the second element of tuple and return it
        """
        logging.info("=====Start Reducer Step=====")
        result = 0
        for _,num in map_output:
            result += num
        logging.info("=====Reducer Step Complete=====")
        return result

    def compute(self):
        return self.reducer(self.mapper())    
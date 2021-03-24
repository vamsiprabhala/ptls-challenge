from src.base.base_map_reducer import MapReducer
from typing import List
import logging

class ComputeUniqueSpans(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        yield span id
        """
        logging.info("=====Start Mapper Step=====")
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.spanId', 'number'):
                yield value
        logging.info("=====Mapper Step Complete=====")

    def reducer(self,map_output) -> int:
        """
        add span id's to a Set and return its length
        """
        logging.info("=====Start Reducer Step=====")
        unique_spans = set()
        for value in map_output:
            unique_spans.add(value)
        logging.info("=====Reducer Step Complete=====")
        return len(unique_spans)   

    def compute(self):
        return self.reducer(self.mapper()) 
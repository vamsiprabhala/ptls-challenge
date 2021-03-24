from src.base.base_map_reducer import MapReducer
from typing import List,Set

class ComputeSpanNames(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        yield span name
        """
        logging.info("=====Mapper Step Complete=====")
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.name', 'string'):
                yield value
        logging.info("=====Mapper Step Complete=====")

    def reducer(self,map_output) -> Set:
        """
        add span name to Set and return it
        """
        logging.info("=====Start Reducer Step=====")
        span_names = set()
        for span_name in map_output:
            span_names.add(span_name)
        logging.info("=====Reducer Step Complete=====")
        return span_names
        

    def compute(self):
        return self.reducer(self.mapper())    
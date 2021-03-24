from src.base.base_map_reducer import MapReducer
from typing import List

class ComputeTraces(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        yield msb,lsb pair for each record
        """
        logging.info("=====Start Mapper Step=====")
        msb, lsb = None, None
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.traceId.mostSignificantBits', 'number'):
                msb = value 
            if (prefix, event) == ('item.traceId.leastSignificantBits', 'number'):
                lsb = value
                yield ((msb,lsb))
                msb, lsb = None, None 
        logging.info("=====Mapper Step Complete=====")


    def reducer(self,map_output) -> int:
        """
        add (msb,lsb) tuple to a set and return its length
        """
        logging.info("=====Start Reducer Step=====")
        unique_traces = set()
        for traceid in map_output:
            unique_traces.add(traceid)
        logging.info("=====Reducer Step Complete=====")
        return len(unique_traces)

    def compute(self):
        return self.reducer(self.mapper())    
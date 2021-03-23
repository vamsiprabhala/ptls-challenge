from src.base.base_map_reducer import MapReducer
from typing import List

class ComputeTraces(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        msb, lsb = None, None
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.traceId.mostSignificantBits', 'number'):
                msb = value 
            if (prefix, event) == ('item.traceId.leastSignificantBits', 'number'):
                lsb = value
                yield ((msb,lsb))
                msb, lsb = None, None 


    def reducer(self,map_output) -> int:
        unique_traces = set()
        for traceid in map_output:
            unique_traces.add(traceid)
        return len(unique_traces)

    def compute(self):
        return self.reducer(self.mapper())    
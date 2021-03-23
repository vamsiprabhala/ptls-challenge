from ptls_challenge.base_map_reducer import MapReducer
from typing import List

class ComputeUniqueSpans(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.spanId', 'number'):
                yield value

    def reducer(self,map_output):
        unique_spans = set()
        for value in map_output:
            unique_spans.add(value)
        return len(unique_spans)   

    def compute(self):
        return self.reducer(self.mapper()) 
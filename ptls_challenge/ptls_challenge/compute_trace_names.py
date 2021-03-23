from ptls_challenge.base_map_reducer import MapReducer
from typing import List,Set

class ComputeTraceNames(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        Name of root span is the name of trace.
        Getting all record keys and only adding the name
        when parentSpanId key doesn't exist for a record
        """
        record_keys = set()
        for prefix,event,value in super().read_split_files():
            record_keys.add(prefix)
            if (prefix, event) == ('item.name', 'string'):
                name = value
            if (prefix, event) == ('item', 'end_map'):
                if 'item.parentSpanId' not in record_keys:
                    yield name
                record_keys = set()

    def reducer(self,map_output) -> Set:
        trace_names = set()
        for trace_name in map_output:
            trace_names.add(trace_name)
        return trace_names

    def compute(self):
        return self.reducer(self.mapper())    
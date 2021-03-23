from src.base.base_map_reducer import MapReducer
from typing import List

class ComputeSpanMessages(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.messageSequenceNumber', 'number'):
                yield (value,1)

    def reducer(self,map_output) -> int:
        result = 0
        for _,num in map_output:
            result += num
        return result

    def compute(self):
        return self.reducer(self.mapper())    
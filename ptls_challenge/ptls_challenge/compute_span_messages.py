from ptls_challenge.base_mapreducer import MapReducer

class GetSpanMessages(MapReducer):

    def __init__(self, split_files: str):
        super().__init__(split_files)
    
    def mapper(self):
        for prefix,event,value in super().read_split_files():
            if (prefix, event) == ('item.messageSequenceNumber', 'number'):
                yield (value,1)

    def reducer(self,map_output):
        result = 0
        for _,num in map_output:
            result += num
        return result

    def compute(self):
        return self.reducer(self.mapper())    
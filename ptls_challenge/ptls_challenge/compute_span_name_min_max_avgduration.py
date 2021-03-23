from ptls_challenge.base_map_reducer import MapReducer
from typing import List,Dict
import heapq

class ComputeSpanNameMinMaxAvgDuration(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        span_durations = dict()
        for prefix,event,value in super().read_split_files():
            if prefix == 'item.spanId':
                spanId = value
            if prefix == 'item.name':
                name = value
            if prefix == 'item.startTimeMicroseconds':
                if spanId in span_durations:
                    duration = span_durations[spanId] - int(value)
                    span_durations[spanId] = duration
                    if duration > 0 and duration < 10**8:
                        yield name,duration
                else:
                    span_durations[spanId] = int(value)
            if prefix == 'item.stopTimeMicroseconds':
                if spanId in span_durations:
                    duration = int(value) - span_durations[spanId]
                    span_durations[spanId] = duration 
                    if duration > 0 and duration < 10**8:
                        yield name,duration
                else:
                    span_durations[spanId] = int(value)

    def reducer(self,map_output) -> Dict:
        """
        avergae duration per span name is computed
        And return a dictionary with the span names of shortest and longest average duration
        """
        span_name_avg_durations = dict()
        for name,duration in map_output:
            if name not in span_name_avg_durations:
                span_name_avg_durations[name] = (duration,1)
            else:
                dur,num = span_name_avg_durations[name] 
                updatedAvg = (num*dur + duration)//(num+1)
                span_name_avg_durations[name] = (updatedAvg,num+1)
        sorted_span_name_avg_durations = sorted(span_name_avg_durations.items(),key=lambda tup: tup[1][0])
        return {'Longest Avg Duration Span Name': sorted_span_name_avg_durations[-1][0],
                'Shortest Avg Duration Span Name': sorted_span_name_avg_durations[0][0],
               }


    def compute(self):
        return self.reducer(self.mapper())    
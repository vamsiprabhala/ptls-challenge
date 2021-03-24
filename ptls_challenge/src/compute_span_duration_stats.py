from src.base.base_map_reducer import MapReducer
from src.config import helpers
from typing import List,Dict
import heapq
import logging

class ComputeSpanDurationStats(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        logging.info("=====Start Mapper Step=====")
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
        logging.info("=====Mapper Step Complete=====")

    def reducer(self,map_output) -> Dict:
        """
        min and max duration -> check the variable against incoming values 
        avg duration         -> sum of all durations // total number of values
        p95 duration
         - maintain a heap equal to the length of 5 percent of values 
         - this number is approximated with the number of records in file//2 
           (because all except 13 spans have 2 records)
         - push to the heap until this length is reached
         - once length is reached, check the top of heap against the incoming value
            - if less pop from heap and push the new value
         - at the end we will have top 5% values on the heap 
           and p95 = top element of this heap 
        """
        logging.info("=====Start Reducer Step=====")
        running_sum = 0
        num = 0
        min_duration = float('inf')
        max_duration = float('-inf')
        length_of_top_5_pct_durations = int((884000//2)*0.05)
        top_5_pct_durations = []
        for _,duration in map_output:
            running_sum += duration 
            num += 1
            if len(top_5_pct_durations) < length_of_top_5_pct_durations:
                heapq.heappush(top_5_pct_durations, duration)
            else:
                if duration > top_5_pct_durations[0]:
                    heapq.heappushpop(top_5_pct_durations, duration)
            if duration < min_duration:
                min_duration = duration
            if duration > max_duration:
                max_duration = duration 
        
        logging.info("=====Reducer Step Complete=====")
        if num > 0:
            return {'min_duration': min_duration, 'max_duration': max_duration, 
                    'avg_duration': running_sum//num, 'p95_duration': top_5_pct_durations[0]
                   }
        else:
            raise Exception("No events found!!")

        

    def compute(self):
        return self.reducer(self.mapper())    
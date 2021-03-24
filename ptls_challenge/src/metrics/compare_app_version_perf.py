from src.base.base_map_reducer import MapReducer
from typing import List
import logging

class CompareAppVersionPerformance(MapReducer):

    def __init__(self, split_files: List[str]):
        super().__init__(split_files)
    
    def mapper(self):
        """
        yield name, duration, appId and country
        """
        logging.info("=====Start Mapper Step=====")
        span_durations = dict()
        for prefix,event,value in super().read_split_files():
            if prefix == 'item.spanId':
                spanId = value
            if prefix == 'item.name':
                name = value
            if prefix == 'item.cesMetadata.oauthAppId':
                appId = value
            if prefix == 'item.cesMetadata.countryCode':
                country = value
            if prefix == 'item.startTimeMicroseconds':
                if spanId in span_durations:
                    duration = span_durations[spanId] - int(value)
                    span_durations[spanId] = duration
                    if duration > 0 and duration < 10**8:
                        yield name,duration,appId,country
                else:
                    span_durations[spanId] = int(value)
            if prefix == 'item.stopTimeMicroseconds':
                if spanId in span_durations:
                    duration = int(value) - span_durations[spanId]
                    span_durations[spanId] = duration 
                    if duration > 0 and duration < 10**8:
                        yield name,duration,appId,country
                else:
                    span_durations[spanId] = int(value)
        logging.info("=====Mapper Step Complete=====")

    def reducer(self,map_output) -> None:
        """
        avergae duration per span name, app version and country is computed
        and comparison is done by these dimensions to report faster/slower 
        """
        logging.info("=====Start Reducer Step=====")
        span_name_country_avg_durations = dict()
        for name,duration,appId,country in map_output:
            if (name,appId,country) not in span_name_country_avg_durations:
                span_name_country_avg_durations[(name,appId,country)] = (duration,1)
            else:
                dur,num = span_name_country_avg_durations[(name,appId,country)] 
                updatedAvg = (num*dur + duration)//(num+1)
                span_name_country_avg_durations[(name,appId,country)] = (updatedAvg,num+1)
        """
        output of span_name_country_avg_durations 
        -> dict of tuple (name,appid,country) to tuple (avg_duration,num_events)
        {('loading-timeline-tweets', '123', 'US'): (297034, 9993), ('authenticating-user', '123', 'US'): (197040, 9999), 
         ...
        }
        Sort the dictionary items by name,appid and average duration in ascending order 
        Compare each tuple with its next and print the message accordingly 
        (only need to look at alternate indices - because of sorted list of tuples)
        """
        sorted_dict = sorted(span_name_country_avg_durations.items(),key=lambda tup: (tup[0][0],tup[0][1],tup[1][0]))
        
        for i in range(0,len(sorted_dict),2):
            print(sorted_dict[i], "is faster and has less average duration compared to", sorted_dict[i+1])
        
        logging.info("=====Reducer Step Complete=====")
        return 


    def compute(self):
        return self.reducer(self.mapper())    
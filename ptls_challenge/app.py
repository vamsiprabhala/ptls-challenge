from ptls_challenge import splitter
from ptls_challenge import base_map_reducer
from ptls_challenge import compute_span_messages
from ptls_challenge import compute_unique_spans
from ptls_challenge import compute_traces
from ptls_challenge import compute_span_names
from ptls_challenge import compute_trace_names
from ptls_challenge import compute_span_duration_stats
from ptls_challenge import compute_span_name_min_max_avgduration
import glob 
import os


if __name__ == '__main__':
    INPUT_FILE_PATH = '/Users/vkprabhala/Desktop/twitter-challenge'
    INPUT_FILE = 'application-trace.json'
    SPLIT_FILE_PATTERN = 'split-*.txt'
    split_obj = splitter.Splitter(INPUT_FILE_PATH, INPUT_FILE)
    #split_obj.splitFile() 
    SPLIT_FILES = glob.glob(os.path.join(INPUT_FILE_PATH,SPLIT_FILE_PATTERN))
    #compute step - map followed by reduce or using a higher order function
    span_msgs = compute_span_messages.ComputeSpanMessages(SPLIT_FILES)
    #print("Span Messages = {}".format(span_msgs.compute()))
    unique_spans = compute_unique_spans.ComputeUniqueSpans(SPLIT_FILES)
    #print("Unique Spans = {}".format(unique_spans.compute()))
    traces = compute_traces.ComputeTraces(SPLIT_FILES)
    #print("Number of Traces = {}".format(traces.compute()))
    span_names = compute_span_names.ComputeSpanNames(SPLIT_FILES)
    #print("Span Names = {}".format(span_names.compute()))
    trace_names = compute_trace_names.ComputeTraceNames(SPLIT_FILES)
    #print("Trace Names = {}".format(trace_names.compute()))
    span_duration_stats = compute_span_duration_stats.ComputeSpanDurationStats(SPLIT_FILES)
    print("Span Duration Stats = {}".format(span_duration_stats.compute()))
    span_name_avgduration_stats = compute_span_name_min_max_avgduration.ComputeSpanNameMinMaxAvgDuration(SPLIT_FILES)
    print("Span Name Avg Duration Stats = {}".format(span_name_avgduration_stats.compute()))




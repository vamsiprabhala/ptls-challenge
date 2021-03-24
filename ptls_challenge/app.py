from src.base import splitter
from src.metrics import compute_span_messages
from src.metrics import compute_unique_spans
from src.metrics import compute_traces
from src.metrics import compute_span_names
from src.metrics import compute_trace_names
from src.metrics import compute_span_duration_stats
from src.metrics import compute_span_name_min_max_avgduration
from src.metrics import compare_app_version_perf
import logging
import glob 
import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", help="Please provide an absolute input JSON file path", type=str)
    args = parser.parse_args()
    INPUT_FILE_PATH = args.file_path
    split_obj = splitter.Splitter(INPUT_FILE_PATH)
    SPLIT_FILES = split_obj.splitFile() 
    logging.info("Split files to be processed -> {}".format(SPLIT_FILES))
    logging.info("**************Start Computing Metrics**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Span Messages**************")
    span_msgs = compute_span_messages.ComputeSpanMessages(SPLIT_FILES)
    logging.info("Span Messages = {}".format(span_msgs.compute()))
    logging.info("**************Computing Metric Span Messages Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Unique Spans**************")
    unique_spans = compute_unique_spans.ComputeUniqueSpans(SPLIT_FILES)
    logging.info("Unique Spans = {}".format(unique_spans.compute()))
    logging.info("**************Computing Metric Unique Spans Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Traces**************")
    traces = compute_traces.ComputeTraces(SPLIT_FILES)
    logging.info("Number of Traces = {}".format(traces.compute()))
    logging.info("**************Computing Metric Traces Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Span Names**************")
    span_names = compute_span_names.ComputeSpanNames(SPLIT_FILES)
    logging.info("Span Names = {}".format(span_names.compute()))
    logging.info("**************Computing Metric Span Names Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Trace names**************")
    trace_names = compute_trace_names.ComputeTraceNames(SPLIT_FILES)
    logging.info("Trace Names = {}".format(trace_names.compute()))
    logging.info("**************Computing Metric Trace names Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Span Duration Statistics**************")
    span_duration_stats = compute_span_duration_stats.ComputeSpanDurationStats(SPLIT_FILES)
    logging.info("Span Duration Stats = {}".format(span_duration_stats.compute()))
    logging.info("**************Computing Metric Span Duration Statistics Complete**************")
    logging.info('='*60)
    logging.info("**************Computing Metric Longest&Shortest Spans**************")
    span_name_avgduration_stats = compute_span_name_min_max_avgduration.ComputeSpanNameMinMaxAvgDuration(SPLIT_FILES)
    logging.info("Span Name Avg Duration Stats = {}".format(span_name_avgduration_stats.compute()))
    logging.info("**************Computing Metric Longest&Shortest Spans Complete**************")
    logging.info('='*60)
    logging.info("**************Comparing App Version Performances**************")
    compare_app_version_perfs = compare_app_version_perf.CompareAppVersionPerformance(SPLIT_FILES)
    logging.info(compare_app_version_perfs.compute())
    logging.info("**************Comparing App Version Performances Complete**************")




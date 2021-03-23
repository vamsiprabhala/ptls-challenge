from ptls_challenge import splitter
from ptls_challenge import base_map_reducer
from ptls_challenge import compute_span_messages
import glob 
import os


if __name__ == '__main__':
    INPUT_FILE_PATH = '/Users/vkprabhala/Desktop/twitter-challenge'
    INPUT_FILE = 'application-trace.json'
    SPLIT_FILE_PATTERN = 'split-*.txt'
    SPLIT_FILES = glob.glob(os.path.join(INPUT_FILE_PATH,SPLIT_FILE_PATTERN))
    split_obj = splitter.Splitter(INPUT_FILE_PATH, INPUT_FILE)
    print(SPLIT_FILES)
    #split_obj.splitFile() 
    #compute step - map followed by reduce or using a higher order function
    #num_span_msgs = getSpanMessages.GetSpanMessages(SPLIT_FILES)
    #print(num_span_msgs.compute())



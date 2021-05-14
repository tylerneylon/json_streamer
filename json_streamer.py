""" json_streamer.py


    Read in a JSON file one piece at a time, saving memory.
    This assumes that the JSON file is a list. It can read in the
    list items one at time, or in blocks.


    Usage example, reading in a file one item at a time:

        from json_streamer import JSONStreamer

        f = open('myfile.json')
        json_streamer = JSONStreamer(f)

        for item in json_streamer:
            print(item)


    Usage example, reading in 100 items in blocks:

        from json_streamer import JSONStreamer

        f = open('myfile.json')
        json_streamer = JSONStreamer(f)

        for block in json_streamer.get_blocks(100):
            for item in block:
                print(item)
"""


# Imports
import json


# Module globals and constants
PAGE_SIZE = 1024 * 1024  # We'll read in one megabyte at a time.
decoder = json.JSONDecoder()


# Class definition
class JSONStreamer(object):
    
    def __init__(self, f):
        self.f = f
        self.buffer = f.read(PAGE_SIZE)
        self._next_index(0)
        assert self.buffer[self.index] == '['
        self._next_index(self.index + 1)
        
    def _read_page(self):
        if self.index > PAGE_SIZE:
            self.buffer = self.buffer[self.index:]
            self.index = 0
        self.buffer += self.f.read(PAGE_SIZE)
        
    def _next_index(self, i):
        skip_set = ' \n\t,'
        self.index = i
        while (self.index == len(self.buffer) or
               self.buffer[self.index] in skip_set):
            if self.index == len(self.buffer):
                self._read_page()
            if self.buffer[self.index] in skip_set:
                self.index += 1
    
    # Return either (obj, False) or (None, True).
    # The second element indicates is_done.
    def _next_obj(self):
       
        if self.buffer[self.index] == ']':
            return None, True
        
        obj = None
        while obj is None:
            try:
                obj, k = decoder.raw_decode(self.buffer[self.index:])
            except json.JSONDecodeError:
                self._read_page()
            
        self._next_index(self.index + k)
        return obj, False    
        
    def __iter__(self):
        return self
    
    def __next__(self):
        obj, is_done = self._next_obj()
        if is_done:
            raise StopIteration
        return obj
    
    def get_blocks(self, block_size):
        block = []
        is_done = False
        while not is_done:
            obj, is_done = self._next_obj()
            if is_done:
                break
            block.append(obj)
            if len(block) == block_size:
                yield block
                block = []
        if block:
            yield block

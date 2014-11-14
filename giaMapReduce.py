__author__ = 'Gia'
import json
from pprint import pprint

class giaMapReduce:

    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value)

    def execute(self, data, mapper, reducer):

        for line in data:
            record = json.loads(line)
            pprint("line is " + line)
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        count = 0
        for i in self.intermediate:

            #print(self.result[count])
            count = count + 1

        jenc = json.JSONEncoder()
        for item in self.result:
            print(jenc.encode(item))
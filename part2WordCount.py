__author__ = 'Gia'
import giaMapReduce
from pprint import pprint
import json
import re

"""
Word Count Example in the Simple Python MapReduce Framework
"""


def mapper(record):

    for key in record:

        book_name_key = key
        book_name_value = record.get(book_name_key)

        for w in book_name_value.split():

            if re.match("^[A-Za-z0-9_-]*$", w):

                mr.emit_intermediate(w, 1)


def reducer(key, list_of_values):

    result = 0

    for i in list_of_values:
        result += i


    mr.emit((key, result))



mr = giaMapReduce.giaMapReduce()

if __name__ == '__main__':

    with open('booksAlt.json') as data_file:
        data = json.load(data_file)
        data_file.close()

    with open ('booksAlt.json') as data_file:
        mr.execute(data_file, mapper, reducer)
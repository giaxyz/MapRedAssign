__author__ = 'Gia'
import giaMapReduceFriend
import json


def mapper(record):

    for pairs in record:

        key = pairs[0]
        value = pairs[1]
        mr.emit_intermediate(key, 1)


def reducer(key, list_of_values):

    result = 0
    for i in list_of_values:
        result += i

    mr.emit((key, result))

mr = giaMapReduceFriend.giaMapReduceFriend()

if __name__ == '__main__':

    data = []
    with open('friends.json') as f:
        for line in f:
            data.append(json.loads(line))


    mr.execute(data, mapper, reducer)

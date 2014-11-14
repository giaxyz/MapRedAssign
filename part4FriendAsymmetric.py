__author__ = 'Gia'
import giaMapReduceFriend
from pprint import pprint
import json

class Mdict(dict):

    def __setitem__(self, key, value):
        """add the given value to the list of values for this key"""
        self.setdefault(key, []).append(value)

def mapper(rec):

    record = Mdict()

    for pairs in rec:
        key = pairs[0]
        value = pairs[1]
        record[key] = value

    all_names = record.keys()

    for key in record:
        key_name = key
        value_friends = (record.get(key_name))

        for friend in value_friends:

            if friend in all_names:

                friendsFriendList = record.get(friend)

                if not key_name in friendsFriendList:

                    mr.emit_intermediate(key_name, friend)

            else:

                mr.emit_intermediate(key_name, friend)


def reducer(key, list_of_values):

    result = list_of_values

    for p in list_of_values:
        mr.emit((key, p))


mr = giaMapReduceFriend.giaMapReduceFriend()

if __name__ == '__main__':

    data = []
    with open('friends.json') as f:
        for line in f:
            data.append(json.loads(line))


    mr.execute(data, mapper, reducer)

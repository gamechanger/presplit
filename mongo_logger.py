import logging
import time

class MongoLogger(object):

    def __init__(self, coll):
        self._coll = coll

    def log_id(self, id, count=0):
        self._coll.save({'_id': id, 'p': True, 'd': time.time(), 'n': count})

    def check_id(self, id):
        return self._coll.find_one({'_id': id, 'p': True})

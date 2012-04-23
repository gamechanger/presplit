import logging

from datetime import datetime, time, timedelta
from objectid import ObjectId

_midnight = time()
_noon = time(hour=12)
_day = timedelta(days=1)

class PreSplitter(object):

    def __init__(self, balancer, shards, logger=None):
        self._balancer = balancer
        self._shards = shards
        self._logger = logger

    def _check_id(self, id):
        return self._logger.check_id(id) if self._logger else True

    def _log_id(self, id, count=0):
        return self._logger.log_id(id, count) if self._logger else None

    def presplit(self, date):
        start_of_day = datetime.combine(date, _midnight)
        middle_of_day = datetime.combine(date, _noon)
        end_of_day = start_of_day + _day

        mins = [ObjectId.from_datetime(d, min=True) for d in
                (start_of_day, middle_of_day, end_of_day)]
        mins = [id for id in mins if not self._check_id(id)]

        if not mins:
            logging.info('have already pre-split all chunks for %s' % date)
            return

        start = min(mins)
        end = ObjectId.from_datetime(end_of_day, max=True)

        count = self._balancer.balance_range(start, end, self._shards)
        for id in mins:
            if id == start:
                self._log_id(id, count)
            else:
                self._log_id(id)

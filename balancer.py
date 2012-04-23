import pymongo
import logging
from objectid import ObjectId

class Balancer(object):
    """
    Provides various load-balancing services for a sharded collection.
    """

    def __init__(self, conn, ns, key):
        self._config = conn.config
        self._admin = conn.admin
        self._ns = ns
        self._key = key

    def chunk_for_key(self, key):
        """
        Returns the chunk that contains the key <key>.
        """
        return self._config.chunks.find_one({'ns': self._ns,
                                             'min.%s' % self._key: {'$lte': key},
                                             'max.%s' % self._key: {'$gt': key}})

    def split_chunk(self, key):
        """
        Wrapper on top of the mongo split command. Finds and returns
        the two resulting chunks.
        """
        self._admin.command('split', self._ns, middle={self._key: key})
        return (self._config.chunks.find_one({'max.%s' % self._key: key}),
                self._config.chunks.find_one({'min.%s' % self._key: key}))

    def move_chunk(self, key, dest):
        """
        Wrapper on top of the mongo moveChunk command.
        """
        try:
            self._admin.command('moveChunk', self._ns,
                                find={self._key: key},
                                to=dest)
        except pymongo.errors.OperationFailure as e:
            logging.warning("Got an OperationFailure.")
            logging.warning("This is likely just Mongo complaining because this shard is already at its destination.")
            logging.warning(e.args)

    def divide_chunk(self, key, divisions):
        """
        Divide the chunk specified by <key> into <divisions> separate
        chunks, each containing an equal key range.
        """
        chunk = self.chunk_for_key(key)
        new_chunks = []
        def divide_chunk_helper(chunk, divisions):
            if divisions < 2:
                new_chunks.append(chunk)
                return
            min = ObjectId(chunk['min'][self._key])
            max = ObjectId(chunk['max'][self._key])
            split = min + (max-min)/divisions
            logging.info('splitting at %s' % split)
            new, remaining = self.split_chunk(split)
            new_chunks.append(new)
            divide_chunk_helper(remaining, divisions-1)
        divide_chunk_helper(chunk, divisions)
        return new_chunks

    def chunks_for_range(self, start, end):
        """
        All of the chunks between the <start> and <end> keys,
        inclusive. Returns a generator.
        """

        min_chunk = self.chunk_for_key(start)
        max_chunk = self.chunk_for_key(end)
        if not min_chunk or not max_chunk:
            return
        if min_chunk['_id'] == max_chunk['_id']:
            yield min_chunk
            return

        other_chunks = self._config.chunks.find({'min.%s' % self._key:
                                                     {'$gt': min_chunk['min'][self._key],
                                                      '$lt': max_chunk['min'][self._key]}})
        other_chunks = other_chunks.sort('min.%s' % self._key)

        yield min_chunk
        for chunk in other_chunks:
            yield chunk
        yield max_chunk

    def balance_range(self, start, end, shards):
        """
        Divide a range of chunks between any number of shards.
        """
        chunks = list(self.chunks_for_range(start, end))
        if not chunks:
            logging.info('no chunks found')
            return

        if chunks[0]['min'][self._key] < start and chunks[0]['max'][self._key] != start:
            chunks[0] = self.split_chunk(start)[1]
        if chunks[-1]['max'][self._key] > end and chunks[-1]['min'][self._key] != end:
            chunks[-1] = self.split_chunk(end)[0]

        total = 0
        for chunk in chunks:
            logging.info('dividing chunk %s' % chunk)
            new_chunks = self.divide_chunk(chunk['min'][self._key], len(shards))
            for i, shard in enumerate(shards):
                total += self.move_chunk(new_chunks[i]['min'][self._key], shard) or 0
        return total

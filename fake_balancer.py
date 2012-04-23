import pymongo
from balancer import Balancer

class FakeBalancer(Balancer):
    """
    Act like a balancer, but make changes to a fake config collection
    instead of the actual sharding config.
    """

    def __init__(self, conn, ns, key, coll_name='fake_config'):
        super(FakeBalancer, self).__init__(conn, ns, key)
        # fake balancer uses fake config
        self._config = conn[coll_name]
        self._connections = {}

    def _collection_for_chunk(self, chunk):
        if chunk['shard'] not in self._connections:
            shard = self._config.shards.find_one({'_id': chunk['shard']})
            self._connections[chunk['shard']] = pymongo.Connection(shard['host'],
                                                                 slave_okay=True)
        return self._connections[chunk['shard']]

    def _count_for_chunk(self, chunk):
        spec = chunk['ns'].split('.')
        db, coll = spec[0], ''.join(spec[1:])
        return self._collection_for_chunk(chunk)[db][coll].find(
            {self._key: {'$gte': chunk['min'][self._key],
                         '$lte': chunk['max'][self._key]}}).count()

    def split_chunk(self, key):
        """
        Simulate the behavior of mongo's split command
        """
        chunk = self.chunk_for_key(key)
        new_chunk = chunk.copy()
        chunk['max'] = new_chunk['min'] = {self._key: key}
        new_chunk['_id'] = '%s-%s_%s' % (self._ns, self._key, repr(key))
        self._config.chunks.save(chunk)
        self._config.chunks.save(new_chunk)
        return chunk, new_chunk

    def move_chunk(self, key, dest):
        """
        Simulate the behavior of mongo's moveChunk command
        """
        chunk = self.chunk_for_key(key)
        chunk['shard'] = dest
        self._config.chunks.save(chunk)
        return self._count_for_chunk(chunk)

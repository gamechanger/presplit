from bson.objectid import ObjectId as BaseObjectId

class ObjectId(BaseObjectId):
    """
    Adds some methods which are useful for dealing with client ids and
    for converting to/from dates and ints
    """

    def __init__(self, oid, client=False):
        self.client = getattr(oid, 'client', None) or client
        super(ObjectId, self).__init__(oid)

    def to_client_id(self):
        remap = [0, 1, 4, 5, 6, 7, 8, 2, 3, 9, 10, 11]
        # do the inverse gc device shuffle, boop boop de doop
        bytes = list(self.__id)
        remapped_bytes = [bytes[remap[i]] for i in range(12)]
        return ObjectId("".join(remapped_bytes), client=True)


    def to_server_id(self):
        remap = [0, 1, 7, 8, 2, 3, 4, 5, 6, 9, 10, 11]
        # do the gc device shuffle, boop boop de doop
        bytes = list(self.__id)
        remapped_bytes = [bytes[remap[i]] for i in range(12)]
        return ObjectId("".join(remapped_bytes), client=False)

    @property
    def generation_time(self):
        if self.client:
            return self.to_server_id().generation_time
        return super(ObjectId, self).generation_time

    def __int__(self):
        return int(str(self), 16)

    @classmethod
    def from_int(cls, x):
        return ObjectId(hex(x)[2:].rstrip('L'))

    def __add__(self, other):
        return self.from_int(int(self) + int(other))

    def __sub__(self, other):
        return int(self) - int(other)

    @classmethod
    def _pop_from_dict(self, d, k):
        v = None
        if k in d:
            v = d[k]
            del d[k]
        return v

    @classmethod
    def from_datetime(cls, *args, **kwargs):
        min = cls._pop_from_dict(kwargs, 'min')
        max = cls._pop_from_dict(kwargs, 'max')

        assert not (min and max), "Both min and max specified"

        oid = ObjectId(BaseObjectId.from_datetime(*args, **kwargs))
        if min or max:
            oid = ObjectId(oid.__id[:2] + ('\x00' if min else '\xff') * 10)
        return oid

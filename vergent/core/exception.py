

class InternalError(Exception):
    pass


class NodeMetaStoreError(Exception):
    pass


class NodeMetaCorrupted(NodeMetaStoreError):
    pass


class NodeMetaVersionMismatch(NodeMetaStoreError):
    pass

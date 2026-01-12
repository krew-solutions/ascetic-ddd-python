__all__ = (
    "ObjectDoesNotExist",
    "ConcurrentUpdate",
)


class ObjectDoesNotExist(Exception):
    pass


class ConcurrentUpdate(Exception):
    pass

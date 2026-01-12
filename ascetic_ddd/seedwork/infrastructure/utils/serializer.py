import base64

try:
    import cPickle as pickle
except ImportError:
    import pickle

__all__ = ('serialize', 'deserialize')


def serialize(val):
    return base64.standard_b64encode(
        pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
    ).decode('utf-8')


def deserialize(val):
    return pickle.loads(base64.standard_b64decode(val.encode('utf-8')))

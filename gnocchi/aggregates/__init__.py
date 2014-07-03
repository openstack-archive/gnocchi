from stevedore import extension

def extensions():
    return extension.ExtensionManager(
        namespace='gnocchi.aggregates',
        invoke_on_load=True,
)

class CustomAggregator(object):

    @staticmethod
    def __init__():
        pass

    @staticmethod
    def compute(data, **params):
        raise NotImplementedError


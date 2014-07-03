import numpy as np
import pandas as pd

from gnocchi import aggregates
from gnocchi.openstack.common import timeutils


class RollingMean(aggregates.CustomAggregator):

    def compute(self, data, **params):
        '''Function that computes a rolling mean
        Parameters
        ----------
        :param data : dict of utc/timestamp value pairs
        :param **params : dict for parameters center and window.
             center should be True/False.
             window must be a frequency string, e.g. '90S'.
             This is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        if 'center' in params:
            center = (params['center'][0].upper() == 'T')
        else:
            center = False
        if 'window' in params:
            window = params['window']
        else:
            raise KeyError('window must be specified for aggregate')

        def rolling_window(x):
            '''For a timestamp x, rolling_window finds all points in data
            that are within a given time window of the initial timestamp x
            and returns the mean of the points.
            '''
            if center:
                dslice = data[x - pd.datetools.to_offset(window).delta / 2:
                                  x + pd.datetools.to_offset(window).delta / 2]
            else:
                dslice = data[x - pd.datetools.to_offset(window).delta:x]
                # start and endpoint are inclusive
                # TODO(atmalagon): modify to make endpoints exclusive
                if dslice.size < 1:
                    return np.nan
                else:
                    return dslice.mean()

        if isinstance(window, basestring):
            data = pd.Series(data)
            idx = pd.DatetimeIndex(data.index)
            data = pd.Series(data, index=idx).sort_index()
            idx = pd.Series(data.index, index=data.index)
            result = idx.apply(rolling_window).dropna()
            return dict((timeutils.strtime(id), val) for id, val in
                    zip(result.index, result))

        else:
            raise TypeError('window must be basestring')


class RollingVariance(aggregates.CustomAggregator):

    def compute(self, data, **params):
        '''Function that computes a rolling variance
        Parameters
        ----------
        :param data : dict of utc timestamp/value pairs
        :param **params : dict for parameters center and window.
             center should be True or False.
             window must be a frequency string, e.g '90s' -
             this is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        if 'center' in params:
            center = (params['center'][0].upper() == 'T')
        else:
            center = False
        if 'window' in params:
            window = params['window']
        else:
            raise KeyError('window must be specified for aggregate')

        def rolling_window(x):
            '''For a timestamp x, rolling_window finds all points in data
            that are within a given time window of the initial timestamp x
            and returns the aggregate func of the points.
            '''
            if center:
                dslice = data[x - pd.datetools.to_offset(window).delta / 2:
                                  x + pd.datetools.to_offset(window).delta / 2]
            else:
                dslice = data[x - pd.datetools.to_offset(window).delta:x]
                # start and endpoint are inclusive
                # TODO(atmalagon): modify to make endpoints exclusive
                if dslice.size < 2:
                    return np.nan
                else:
                    return dslice.std()

        if isinstance(window, basestring):
            data = pd.Series(data)
            idx = pd.DatetimeIndex(data.index)
            data = pd.Series(data, index=idx).sort_index()
            idx = pd.Series(data.index, index=data.index)
            result = idx.apply(rolling_window).dropna()
            return dict((timeutils.strtime(id), val) for id, val in
                    zip(result.index, result))

        else:
            raise TypeError('window must be basestring')


class EWMA(aggregates.CustomAggregator):

    def compute(self, data, **params):
        '''Computes the exponentially weighted average of a time series.
        Parameters
        ----------
        :param data: dict of utc timestamp/value pairs
        :param **params: dict for parameter window
        window must be frequency string, e.g. '90s'
        Returns
        -------
        dict of string timestamp/aggregated value pairs
        '''
        try:
            window = float(params['window'][:-1])
            data = pd.Series(data)
            idx = pd.DatetimeIndex(data.index)
            data = pd.Series(data, index=idx).sort_index()
            result = []
            result.append(data[0])
            for j in range(1, len(data)):
                delta = (data.index[j - 1] - data.index[j]).total_seconds()
                # TODO(atmalagon): generalize this so it doesn't just assume
                # window is in seconds
                w = np.exp(delta / window)
                result.append(result[-1] * w + data[j] * (1 - w))

            result = pd.Series(result, index=data.index).dropna()
            return dict((timeutils.strtime(id), val) for id, val in
                    zip(result.index, result))
        except KeyError:
            raise KeyError('window must be specified for aggregate')

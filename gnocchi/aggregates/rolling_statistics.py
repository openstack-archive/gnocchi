import numpy as np
import pandas as pd

from gnocchi import aggregates
from gnocchi.openstack.common import timeutils


class RollingMean(aggregates.CustomAggregator):

    def compute(self, data, **params):
        '''Function that computes a rolling mean
        Parameters
        ----------
        :param data : Series
        :param window : int or string
             If int is passed, window is the number of observations used for
             calculating the statistic, as defined by the function
             pd.rolling_mean().
             If a string is passed, it must be a frequency string, e.g. '90S'.
             This is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        for k in params:
            if k == 'center':
                center = (params[k][0].upper() == 'T')
            if k == 'window':
                window = params[k]

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
        :param data : Series
        :param window : int or string
             If int is passed, window is the number of observations used for
             calculating the statistic, as defined by the function
             pd.rolling_mean().
             If a string is passed, it must be a frequency string, e.g. '90S'.
             This is internally converted into a DateOffset object,
             representing the window size.
         Returns
         -------
         dict of string timestamp and aggregated value pairs
         '''
        for k in params:
            if k == 'center':
                center = (params[k][0].upper() == 'T')
            if k == 'window':
                window = params[k]

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

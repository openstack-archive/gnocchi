import numpy as np
import pandas as pd

def rolling_mean(data, window, center=False):
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
    Series
    '''
    def rolling_window(x):
        '''For a timestamp x, rolling_window finds all points in data
        that are within a given time window of the initial timestamp x
        and returns the mean of the points.'''
        if center:
            dslice = data[x - pd.datetools.to_offset(window).delta / 2:
                         x + pd.datetools.to_offset(window).delta / 2]
        else:
            dslice = data[x - pd.datetools.to_offset(window).delta:x]
                # start and endpoint are inclusive
                # TODO (atmalagon): modify to make endpoints exclusive
        if dslice.size < 1:
            return np.nan
        else:
            return dslice.mean()

    if isinstance(window, int):
        dfout = pd.rolling_mean(data, window, center=center)
    elif isinstance(window, basestring):
        idx = pd.Series(data.index, index=data.index)
        result = idx.apply(rolling_window)
    return result.dropna()

import pandas as pd
import numpy as np
#originally from stackoverflow.com/questions/15771472/pandas-rolling-\
#mean-by-time-interval solution by user2689410
#see also this post: stackoverflow.com/questions/14300768/pandas-rolling-\
#computation-with-window-based-on-values-instead-of-counts


def rolling_mean(data, window, min_periods=1, center=False):
    '''Function that computes a rolling mean

    Parameters
    ----------
    :param data : DataFrame or Series
           If a DataFrame is passed, the rolling_mean is computed for all
           columns.
    :param window : int or string
             If int is passed, window is the number of observations used for
             calculating the statistic, as defined by the function
             pd.rolling_mean().
             If a string is passed, it must be a frequency string, e.g. '90S'.
             This is internally converted into a DateOffset object,
             representing the window size.
    :param min_periods : int
                  Minimum number of observations in window required to have
                  a value.

    Returns
    -------
    Series or DataFrame, if more than one column
    '''
    def f(x):
        '''Function to apply that actually computes the rolling mean.'''
        if center is False:
            dslice = col[x - pd.datetools.to_offset(window).delta:x]
                # adding a microsecond because when slicing with labels
                # start and endpoint are inclusive <-- taken out (atmalagon)
        else:
            dslice = col[x - pd.datetools.to_offset(window).delta/2:
                         x + pd.datetools.to_offset(window).delta/2]
        if dslice.size < min_periods:
            return np.nan
        else:
            return dslice.mean()

    data = pd.DataFrame(data.copy())
    dfout = pd.DataFrame()
    if isinstance(window, int):
        dfout = pd.rolling_mean(data, window, min_periods=min_periods,
                                center=center)
    elif isinstance(window, basestring):
        idx = pd.Series(data.index.to_pydatetime(), index=data.index)
        for colname, col in data.iterkv():
            result = idx.apply(f)
            result.name = colname
            dfout = dfout.join(result, how='outer')
    if dfout.columns.size == 1:
        dfout = dfout.ix[:, 0]
    return dfout

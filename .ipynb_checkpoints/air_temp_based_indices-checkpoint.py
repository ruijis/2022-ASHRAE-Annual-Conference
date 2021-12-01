import pymortar
import pandas as pd

def range_outlier(md, ss, ws, sd, ed, sh, eh, sl, su, wl, wu):
    """
    Calculate the percentage of normal occupied time outside a specified temeprature range.
    The normal occupied days is Monday to Friday but the occupied time can be specified.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    ss : int
         summer start month, e.g. 5
    ws : int
         summer start month, e.g. 11
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    sl : float
         lower bound of the tempearture range in summer, with default F unit
    su : float
         upper bound of the temperature range in summer, with default F unit
    wl : float
         lower bound of the tempearture range in winter, with default F unit
    wu : float
         upper bound of the temperature range in winter, with default F unit

    Returns
    ----------
    p : float
        percentage of outside range time
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['mo'] = pd.to_datetime(df['time']).dt.month
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # split the occupied data to the summer and  winter
    df_occ_sum = df_occ[(df_occ['mo'] >= ss) & (df_occ['mo'] <= (ws-1))]
    df_occ_win = df_occ[(df_occ['mo'] >= ws) | (df_occ['mo'] <= (ss-1))]
    # create df that is lower or upper the temperature range
    df_sum_out = df_occ_sum[(df_occ_sum['value'] < sl) | 
                            (df_occ_sum['value'] > su)]
    df_win_out = df_occ_win[(df_occ_win['value'] < wl) |
                           (df_occ_win['value'] > wu)]
    # the number of summer and winter occupied time
    n_occ_all = len(df_occ_sum) + len(df_occ_win)
    # Calculate the percentage of occupied time outside the temeprature range
    p = (len(df_sum_out) + len(df_win_out)) / n_occ_all if n_occ_all != 0 else 0
    return round(p, 2)


def daily_range_outlier(md, sd, ed, sh, eh, th):
    """
    Calculate the percentage of occupied days when temp range is outside the threshold.
    The occupied days is Monday to Friday but the occupied time can be specified.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    th : float
         threshold of daily temperature range, with default F unit
        
    Returns
    ----------
    p : float
        percentage of the time
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['da'] = pd.to_datetime(df['time']).dt.date
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # calculate occupied daily temperature range by max minus min
    df_occ_max = df_occ.groupby(['da']).max()
    df_occ_min = df_occ.groupby(['da']).min()
    # add a new column to the df_max called range
    df_occ_max['range'] = df_occ_max['value'] - df_occ_min['value']
    # create a new df containing rows that are out of the threshold
    df_out = df_occ_max[(df_occ_max['range'] > th)]
    p = len(df_out) / len(df_occ) if len(df_occ) != 0 else 0
    return round(p, 2)

def combined_outlier(ro, dr):
    """
    Calculate the combined index of range outlier and daily range outlier.
    
    Parameters
    ----------
    ro : float
         range outlier index value
    dr : float
         daily range outlier index value

    Returns
    ----------
    p : float
        percentage of combined index
    """
    p = (ro + dr)/2
    return round(p, 2)

def degree_hours(md, ss, ws, sd, ed, sh, eh, sl, su, wl, wu):
    """
    Calculate the product sum of weighted factors and exposure time time.

    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    ss : int
         summer start month, e.g. 5
    ws : int
         summer start month, e.g. 11
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    sl : float
         lower bound of the tempearture range in summer, with default F unit
    su : float
         upper bound of the temperature range in summer, with default F unit
    wl : float
         lower bound of the tempearture range in winter, with default F unit
    wu : float
         upper bound of the temperature range in winter, with default F unit

    Returns
    ----------
    ps : float
         degree hours
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['mo'] = pd.to_datetime(df['time']).dt.month
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # split the occupied data to the summer and  winter
    df_occ_sum = df_occ[(df_occ['mo'] >= ss) & (df_occ['mo'] <= (ws-1))]
    df_occ_win = df_occ[(df_occ['mo'] >= ws) | (df_occ['mo'] <= (ss-1))]
    # overheating and overcooling rows in summer and winter
    df_sum_oc = df_occ_sum[(df_occ_sum['value'] < sl)]
    df_sum_oh = df_occ_sum[(df_occ_sum['value'] > su)]
    df_win_oc = df_occ_win[(df_occ_win['value'] < wl)]
    df_win_oh = df_occ_win[(df_occ_win['value'] > wu)]
    # magnitude of overheating and overcooling in summer and winter
    sum_oc_diff = (sl - df_sum_oc['value']).sum()
    sum_oh_diff = (df_sum_oh['value'] - su).sum()
    win_oc_diff = (wl - df_win_oc['value']).sum()
    win_oh_diff = (df_win_oh['value'] - wu).sum()
    # sum and then multiple one hour
    ps = (sum_oc_diff + sum_oh_diff + win_oc_diff + win_oh_diff) * (15/60)
    return round(ps, 2)


def temp_mean(md, sd, ed, sh, eh):
    """
    Calculate mean value of the temperature at occupied time.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    
    Returns
    ----------
    m : float
        mean value of the tempearture
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # Calculate mean value of the temperature from the new datafram
    m = df_occ['value'].mean()
    return round(m, 2)

def temp_var(md, sd, ed, sh, eh):
    """
    Calculate variance of occupied hourly average temperature data.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    
    Returns
    ----------
    v : float
        variance of occupied hourly average temperature data
    """
    
    
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['da'] = pd.to_datetime(df['time']).dt.date
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # get hourly average data by grouping by date frist and hour, then mean
    df_hrs = df_occ.groupby(['da', 'hr']).mean()
    # calculate variance of occupied hourly average temperature data.
    v = df_hrs['value'].var()
    return round(v, 2)

def overcooling_outlier(md, ss, ws, sd, ed, sh, eh, sl, wl):
    """
    Calculate the percentage of normal occupied time lower than a specified temeprature range.
    The normal occupied days is Monday to Friday but the occupied time can be specified.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    ss : int
         summer start month, e.g. 5
    ws : int
         summer start month, e.g. 11
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    sl : float
         lower bound of the temperature range in summer, with default F unit
    wl : float
         lower bound of the temperature range in winter, with default F unit

    Returns
    ----------
    p : float
        percentage of outside range time
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['mo'] = pd.to_datetime(df['time']).dt.month
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # split the occupied data to the summer and  winter
    df_occ_sum = df_occ[(df_occ['mo'] >= ss) & (df_occ['mo'] <= (ws-1))]
    df_occ_win = df_occ[(df_occ['mo'] >= ws) | (df_occ['mo'] <= (ss-1))]
    # create df that is lower or upper the temperature range
    df_sum_out = df_occ_sum[(df_occ_sum['value'] < sl)]
    df_win_out = df_occ_win[(df_occ_win['value'] < wl)]
    # the number of summer and winter occupied time
    n_occ_all = len(df_occ_sum) + len(df_occ_win)
    # Calculate the percentage of occupied time outside the temeprature range
    p = (len(df_sum_out) + len(df_win_out)) / n_occ_all if n_occ_all != 0 else 0
    return round(p, 2)

def overheating_outlier(md, ss, ws, sd, ed, sh, eh, su, wu):
    """
    Calculate the percentage of normal occupied time higher than a specified temeprature range.
    The normal occupied days is Monday to Friday but the occupied time can be specified.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    ss : int
         summer start month, e.g. 5
    ws : int
         summer start month, e.g. 11
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    su : float
         upper bound of the temperature range in summer, with default F unit
    wu : float
         upper bound of the temperature range in winter, with default F unit

    Returns
    ----------
    p : float
        percentage of outside range time
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['mo'] = pd.to_datetime(df['time']).dt.month
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # split the occupied data to the summer and winter
    df_occ_sum = df_occ[(df_occ['mo'] >= ss) & (df_occ['mo'] <= (ws-1))]
    df_occ_win = df_occ[(df_occ['mo'] >= ws) | (df_occ['mo'] <= (ss-1))]
    # create df that is higher than the upper bound of the temperature range
    df_sum_out = df_occ_sum[(df_occ_sum['value'] > su)]
    df_win_out = df_occ_win[(df_occ_win['value'] > wu)]
    # the number of summer and winter occupied time
    n_occ_all = len(df_occ_sum) + len(df_occ_win)
    # Calculate the percentage of occupied time outside the temeprature range
    p = (len(df_sum_out) + len(df_win_out)) / n_occ_all if n_occ_all != 0 else 0
    return round(p, 2)
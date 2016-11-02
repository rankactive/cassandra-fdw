from datetime import datetime, date, time, timedelta
from cStringIO import StringIO

def parse_time_string(str_time):
    mode = 1
    idx = str_time.find('+')
    if idx >= 0:
        time_string = str_time[:idx].strip()
        tz_string = str_time[idx + 1:]
    else:
        idx = str_time.find('-')
        if idx >= 0:
            mode = -1
            time_string = str_time[:idx].strip()
            tz_string = str_time[idx + 1:]
        else:
            time_string = str_time
            tz_string = None
    
    format = '%H:%M:%S'
    idx = time_string.find('.')
    if idx >= 0:
        format += '.%f'
    format += " %Z"
    time_string += " UTC"
    dt = datetime.strptime(time_string, format)
    if tz_string is None:
        return dt.time()
    tz_time = get_tz_time(tz_string)
    return (dt - timedelta(hours=tz_time['hours'] * mode, minutes=tz_time['minutes'] * mode)).time()

def parse_date_string(str_date):
    year = 0
    month = 0
    day = 0
    hour = 0
    minute = 0
    second = 0
    microsecond = 0
    tz_hour = 0
    tz_minute = 0
    state = 0
    file_str = StringIO()
    for c in str_date:
        if c == ' ' and state != 2:
            continue
        #parse year
        if state == 0: 
            if c != '-':
                file_str.write(c)
            else:
                year = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
        #parse month
        elif state == 1:
            if c != '-':
                file_str.write(c)
            else:
                month = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
        #parse day
        elif state == 2:
            if c != ' ':
                file_str.write(c)
            else:
                day = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
        #parse hour
        elif state == 3:
            if c != ':':
                file_str.write(c)
            else:
                hour = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
        #parse minute
        elif state == 4:
            if c != ':':
                file_str.write(c)
            else:
                minute = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
        #parse second
        elif state == 5:
            if c != '.' and c != '+' and c != '-':
                file_str.write(c)
            else:
                second = int(file_str.getvalue())
                file_str.close()
                file_str = StringIO()
                state += 1
                if c == '+' or c == '-':
                    file_str.write(c)
                    state += 1
        elif state == 6:
            if c != '+' and c != '-':
                file_str.write(c)
            else:
                microsecond = int(float('0.' + file_str.getvalue()) * 1000000)
                file_str.close()
                file_str = StringIO()
                if c == '+' or c == '-':
                    file_str.write(c)
                    state += 1
        elif state == 7:
            file_str.write(c)
    
    tzmode = 1
    if state == 2:
        day = int(file_str.getvalue())
    elif state == 5:
        second = int(file_str.getvalue())
    elif state == 6:
        microsecond = int(float('0.' + file_str.getvalue()) * 1000000)
    elif state == 7:
        tzstr = file_str.getvalue()
        if tzstr[0] == '-':
            tzmode = -1
        tztime = get_tz_time(tzstr[1:])
        tz_hour = tztime['hours']
        tz_minute = tztime['minutes']
    else:
        raise ValueError('incorrect datetime format')
    
    if year == 0 or month == 0 or day == 0:
        raise ValueError('incorrect datetime format')

    file_str.close()
    import pytz
    dt = datetime(year, month, day, hour, minute, second, microsecond, pytz.utc)
    if tz_hour == 0 and tz_minute == 0:
        return dt
    return dt - timedelta(hours=tz_hour * tzmode, minutes=tz_minute * tzmode)

def get_tz_time(str_tz_time):
    parts = str_tz_time.split(u':')
    result = {}
    result['hours'] = int(parts[0])
    if len(parts) > 1:
        result['minutes'] = int(parts[1])
    else:
        result['minutes'] = 0
    return result
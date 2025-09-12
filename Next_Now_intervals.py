import datetime

def round_to_next_interval(interval_minutes):
    interval_minutes = int(interval_minutes)
    now = datetime.datetime.now()
    base = datetime.datetime.combine(now.date(), datetime.time(0, 15))
    elapsed = (now - base).total_seconds()

    if elapsed < 0:
        return base.strftime("%Y-%m-%d %H:%M:%S"), None

    intervals_passed = int(elapsed // (interval_minutes * 60))
    now_interval = base + datetime.timedelta(minutes=intervals_passed * interval_minutes)
    next_interval = now_interval + datetime.timedelta(minutes=interval_minutes)

    return now_interval.strftime("%Y-%m-%d %H:%M:%S"), next_interval.strftime("%Y-%m-%d %H:%M:%S")

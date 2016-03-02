import datetime


# takes a CSV file with format: district, side, dow, date1, date2, date3, date4, date5, date6, date7, date8, date9
for d in data:
    code = "CMB-SSWP-{}-{}".format(d[0], d[1])
    desc = "STREET SWEEPING 08:00-14:00 {}".format(d[2])
    periods = []
    for x in d[3:]:
        mo, da = x.split("-")
        dt = datetime.datetime(2016, int(mo), int(da))
        periods.append([(dt - datetime.timedelta(days=6)).strftime("%m-%d"),
            (dt + datetime.timedelta(days=1)).strftime("%m-%d")])
    outdata.append([code, desc, ";".join([",".join(x) for x in periods]), "",
        8.0, 14.0, 6.0, 1 if "Mon" in d[2] else "", 1 if "Tue" in d[2] else "",
        1 if "Wed" in d[2] else "", 1 if "Thu" in d[2] else "", 1 if "Fri" in d[2] else "",
        "", "", "sweeping"])

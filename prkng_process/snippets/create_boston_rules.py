import calendar
import datetime


dow = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
crd = ["1st", "2nd", "3rd", "4th", "5th"]
for d in data:
    startTimeRaw = d[6][:5] if d[6][:5] != "00:01" else "00:00"
    endTimeRaw = d[7][:5] if d[7][:5] != "00:00" else "24:00"
    code = "BOS-SSWP-{}".format(d[0])
    desc = "STREET SWEEPING {}-{} {} - {}".format(startTimeRaw, endTimeRaw,
        "DAILY" if d[16] == "True" else " ".join([dow[x[0]] for x in enumerate(d[17:24]) if x[1] == "True"]),
        "YEAR ROUND" if d[-1] == "True" else ("WEEKLY" if all([x == "True" for x in d[11:16]]) else \
            (" ".join([crd[x[0]] for x in enumerate(d[11:16]) if x[1] == "True"]) + " WEEKS")))
    startTime = float(startTimeRaw[:2]) + (float(startTimeRaw[3:5]) / 60.0 if startTimeRaw[3:5] != "00" else 0.0)
    endTime = float(endTimeRaw[:2]) + (float(endTimeRaw[3:5]) / 60.0 if endTimeRaw[3:5] != "00" else 0.0)
    periods = []
    yearRange = range(1,13) if d[-1] == "True" else range(4,12)
    if all([x == "True" for x in d[11:16]]):
        periods = [["04-01", "11-30"]]
    else:
        for mo in yearRange:
            cal = calendar.monthcalendar(2016, mo)
            for we in range(1,6):
                if d[10 + we] == "True":
                    for da in range(1,8):
                        if d[16 + da] == "True":
                            if cal[we - 1][da - 1]:
                                dt = datetime.datetime(2016, mo, cal[we - 1][da - 1])
                                periods.append([(dt - datetime.timedelta(days=6)).strftime("%m-%d"),
                                    (dt + datetime.timedelta(days=1)).strftime("%m-%d")])
    outdata.append([code, desc, ";".join([",".join(x) for x in periods]) if d[-1] != "True" else "", "",
        startTime, endTime, endTime - startTime, 1 if d[17] == "True" and d[16] != "True" else "",
        1 if d[18] == "True" and d[16] != "True" else "", 1 if d[19] == "True" and d[16] != "True" else "",
        1 if d[20] == "True" and d[16] != "True" else "", 1 if d[21] == "True" and d[16] != "True" else "",
        1 if d[22] == "True" and d[16] != "True" else "", 1 if d[23] == "True" and d[16] != "True" else "",
        1 if d[16] == "True" else "", "", "sweeping"])

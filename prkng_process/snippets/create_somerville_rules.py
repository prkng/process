import calendar
import datetime


dow = ["", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
crd = ["", "1st", "2nd", "3rd", "4th", "5th"]
for a in enumerate(data):
    d = a[1]
    startTime, endTime = float(d[2]), float(d[3])
    prettyStartTime = str(int(startTime)) + ":" + (str(int((startTime * 60) % 60)) if startTime % 1 else "00")
    prettyEndTime = str(int(endTime)) + ":" + (str(int((endTime * 60) % 60)) if endTime % 1 else "00")
    code = "SOM-SSWP-{}".format(a[0] + 1)
    desc = "STREET SWEEPING {}-{} {} {}".format(prettyStartTime, prettyEndTime, d[4],
        "WEEKLY" if not d[5] else (" ".join([crd[int(x)] for x in d[5].split(",")]) + " WEEKS"))
    periods = []
    if not d[5]:
        periods = [["04-01", "12-31"]]
    else:
        for mo in range(4,13):
            cal = calendar.monthcalendar(2016, mo)
            for we in range(1,6):
                if str(we) in d[5]:
                    for da in range(1,8):
                        if dow[da] == d[4]:
                            if cal[we - 1][da - 1]:
                                dt = datetime.datetime(2016, mo, cal[we - 1][da - 1])
                                periods.append([(dt - datetime.timedelta(days=6)).strftime("%m-%d"),
                                    (dt + datetime.timedelta(days=1)).strftime("%m-%d")])
    if startTime > endTime:
        outdata.append([code, desc, ";".join([",".join(x) for x in periods]), "",
            startTime, 24.0, 24.0 - startTime, 1 if d[4] == "Monday" else "",
            1 if d[4] == "Tuesday" else "", 1 if d[4] == "Wednesday" else "",
            1 if d[4] == "Thursday" else "", 1 if d[4] == "Friday" else "",
            1 if d[4] == "Saturday" else "", 1 if d[4] == "Sunday" else "",
            "", "", "sweeping"])
        startTime = 0.0
    outdata.append([code, desc, ";".join([",".join(x) for x in periods]), "",
        startTime, endTime, endTime - startTime, 1 if d[4] == "Monday" else "",
        1 if d[4] == "Tuesday" else "", 1 if d[4] == "Wednesday" else "",
        1 if d[4] == "Thursday" else "", 1 if d[4] == "Friday" else "",
        1 if d[4] == "Saturday" else "", 1 if d[4] == "Sunday" else "",
        "", "", "sweeping"])

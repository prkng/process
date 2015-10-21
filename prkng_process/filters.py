# -*- coding: utf-8 -*-
from collections import namedtuple, defaultdict
from itertools import groupby


def group_rules(rules):
    """
    group rules having the same code and contructs an array of
    parking time for each day
    """
    singles = namedtuple('singles', (
        'code', 'description', 'season_start', 'season_end',
        'time_max_parking', 'agenda', 'special_days', 'metered', 'restrict_typ', 'permit_no'
    ))

    results = []
    days = ('lun', 'mar', 'mer', 'jeu', 'ven', 'sam', 'dim')

    for code, group in groupby(rules, lambda x: (x.code, x.season_start, x.season_end, x.time_max_parking)):

        day_dict = defaultdict(list)

        for part in group:
            for numday, day in enumerate(days, start=1):
                isok = getattr(part, day) or part.daily
                if not isok:
                    continue
                # others cases
                if part.time_end:
                    day_dict[numday].append([part.time_start, part.time_end])

                elif part.time_duration:
                    fdl, ndays, ldf = split_time_range(part.time_start, part.time_duration)
                    # first day
                    day_dict[numday].append([part.time_start, part.time_start + fdl])

                    for inter_day in range(1, ndays + 1):
                        day_dict[numday + inter_day].append([0, 24])
                    # last day
                    if ldf != 0:
                        day_dict[numday].append([0, ldf])

                else:
                    day_dict[numday].append([0, 24])

        # add an empty list for empty days
        for numday, day in enumerate(days, start=1):
            if not day_dict[numday]:
                day_dict[numday] = []

        results.append(singles(
            part.code,
            part.description,
            part.season_start,
            part.season_end,
            part.time_max_parking,
            dict(day_dict),
            part.special_days,
            part.metered == 1,
            part.restrict_typ,
            part.permit_no
        ))

    return results


def split_time_range(start_time, duration):
    """
    Given a start time and a duration, returns a 3-tuple containing
    the time left for the current day, a number of plain day left, a number of hours left
    for the last day
    """
    if start_time + duration <= 24:
        # end is inside the first day
        return duration, 0, 0

    time_left_first = 24 - start_time
    plain_days = (duration - time_left_first) // 24
    time_left_last = (duration - time_left_first) % 24
    return time_left_first, int(plain_days), time_left_last

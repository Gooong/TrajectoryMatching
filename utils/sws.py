import numpy as np
import itertools
import operator
import math


# calculate the distance according to lon,lat
def haversine(lon1, lat1, lon2, lat2):
    lon1 = math.radians(lon1)
    lat1 = math.radians(lat1)
    lon2 = math.radians(lon2)
    lat2 = math.radians(lat2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371
    return c * r * 1000


def psf(distance):
    if distance > 3500:
        return 0
    elif distance <= 1500:
        return 1
    else:
        return 1 - (distance - 1500) / 2000


def sws(record1, record2):
    """
    Space Weighted Similarity
    :param record1: [[x1,x2,...],[y1,y2,...],[t1,t2,...]]
    :param record2: [[x1,x2,...],[y1,y2,...],[t1,t2,...]]
    :return: the space weighted similarity score
    """
    assert len(record1[0]) > 1 and len(record2[0]) > 1

    xl1, yl1, tl1 = record1
    xl2, yl2, tl2 = record2

    start = max(tl1[0], tl2[0])
    end = min(tl1[-1], tl2[-1])
    total_time = sorted(set(tl1 + tl2))
    common_time = list(filter(lambda x: x >= start and x <= end, total_time))
    x1_list = np.interp(common_time, tl1, xl1)
    y1_list = np.interp(common_time, tl1, yl1)
    x2_list = np.interp(common_time, tl2, xl2)
    y2_list = np.interp(common_time, tl2, yl2)

    distance_list = list(map(haversine, x1_list, y1_list, x2_list, y2_list))
    similarity_list = list(map(psf, distance_list))
    delta_distance_1 = list(map(haversine, x1_list[1:], y1_list[1:], x1_list, y1_list))
    final_delta_distance = list(
        map(operator.add, itertools.chain([0], delta_distance_1), itertools.chain(delta_distance_1, [0])))
    score = sum(map(operator.mul, final_delta_distance, similarity_list)) / 2
    return score
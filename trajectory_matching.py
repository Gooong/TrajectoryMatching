from pyspark import SparkContext, SparkConf
import os
import math
import heapq

# os.environ['JAVA_HOME'] = "/home/gongxr/jdk1.8.0_191"
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3.5"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3.5"

# path to your interpreter, recommend the pypy interpreter: https://pypy.org/
# os.environ['PYSPARK_DRIVER_PYTHON'] = "/root/project/pypy/bin/python3"
# os.environ['PYSPARK_PYTHON'] = "/root/project/pypy/bin/python3"
# os.environ['JAVA_HOME'] = "/root/project/jdk1.8.0_191"

# the space and time range of the trajectories
box = [115.5, 39, 0, 117.5, 41, 24 * 60 * 60]
# the segments are partitioned into nx*ny*nt segments
nx = 80
ny = 110
nt = 72

xmin, ymin, tmin, xmax, ymax, tmax = box
wx = (xmax - xmin) / nx
wy = (ymax - ymin) / ny
wt = (tmax - tmin) / nt
# the radius of space-time buffer
distance_threshold = 2000


def line2pair(line):
    l = line.split(' ')
    id = int(l[0])
    seg = list(map(float, l[1:]))
    return id, seg


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


def _preprocess(seg):
    x0, y0, t0, x1, y1, t1 = seg
    _x0 = (x0 - xmin) / wx
    _y0 = (y0 - ymin) / wy
    _t0 = (t0 - tmin) / wt
    _x1 = (x1 - xmin) / wx
    _y1 = (y1 - ymin) / wy
    _t1 = (t1 - tmin) / wt

    return _x0, _y0, _t0, _x1, _y1, _t1


def _get_point(seg, delta):
    x0, y0, t0, x1, y1, t1 = seg
    return [x0 * (1 - delta) + x1 * delta, \
            y0 * (1 - delta) + y1 * delta, \
            t0 * (1 - delta) + t1 * delta]

# get the space-time cells that intersects with the segment
def intersect_boxes(seg):
    _x0, _y0, _t0, _x1, _y1, _t1 = _preprocess(seg)

    delta_list = []
    x_inc = 0
    if _x0 < _x1:
        x_inc = 1
        _dx = _x1 - _x0
        for _x in range(math.floor(_x0) + 1, math.ceil(_x1)):
            delta = (_x - _x0) / _dx
            # print(delta)
            delta_list.append((delta, 'x'))

    elif _x0 > _x1:
        x_inc = -1
        _dx = _x0 - _x1
        for _x in range(math.floor(_x1) + 1, math.ceil(_x0)):
            delta = 1 - (_x - _x1) / _dx
            delta_list.append((delta, 'x'))

    y_inc = 0
    if _y0 < _y1:
        y_inc = 1
        _dy = _y1 - _y0
        for _y in range(math.floor(_y0) + 1, math.ceil(_y1)):
            delta = (_y - _y0) / _dy
            delta_list.append((delta, 'y'))

    elif _y0 > _y1:
        y_inc = -1
        _dy = _y0 - _y1
        for _y in range(math.floor(_y1) + 1, math.ceil(_y0)):
            delta = 1 - (_y - _y1) / _dy
            delta_list.append((delta, 'y'))

    t_inc = 0
    if _t0 < _t1:
        t_inc = 1
        _dt = _t1 - _t0
        for _t in range(math.floor(_t0) + 1, math.ceil(_t1)):
            delta = (_t - _t0) / _dt
            delta_list.append((delta, 't'))

    elif _t0 > _t1:
        t_inc = -1
        _dt = _t0 - _t1
        for _t in range(math.floor(_t1) + 1, math.ceil(_t0)):
            delta = 1 - (_t - _t1) / _dt
            delta_list.append((delta, 't'))

    delta_list = list(sorted(delta_list))
    # print(delta_list)

    c_x = math.floor(_x0)
    c_y = math.floor(_y0)
    c_t = math.floor(_t0)
    cells = [(c_x, c_y, c_t)]
    for delta in delta_list:
        if delta[1] == 'x':
            c_x += x_inc
        elif delta[1] == 'y':
            c_y += y_inc
        else:
            c_t += t_inc
        cells.append((c_x, c_y, c_t))

    return cells

# intersected partition
def flat_idseg(pair):
    tra_id, seg = pair
    boxid_idsseg = []
    for box_id in intersect_boxes(seg):
        boxid_idsseg.append((box_id, (tra_id, seg)))
    return boxid_idsseg

# buffered partiotion
def flat_idseg_with_bounds(pair):
    tra_id, seg = pair
    boxid_idsseg = []
    boxes = set()
    for box_id in intersect_boxes(seg):
        boxes.update(near_box(box_id))

    for box_id in boxes:
        boxid_idsseg.append((box_id, (tra_id, seg)))
    return boxid_idsseg


def near_box(box_id):
    x, y, t = box_id
    for delta_x in [-1, 0, 1]:
        for delta_y in [-1, 0, 1]:
            yield x + delta_x, y + delta_y, t


def chunkify(lst, n):
    return [lst[i::n] for i in range(n)]


def _distance_func(distance):
    if distance > 2000:
        return 0
    elif distance <= 1000:
        return 1
    else:
        return 1 - (distance - 1000) / 1000


def score(box_id, seg_a, seg_b):
    ta0 = seg_a[2]
    tb0 = seg_b[2]
    ta1 = seg_a[5]
    tb1 = seg_b[5]
    _t0 = max(ta0, tb0)
    _t1 = min(ta1, tb1)

    if _t0 >= _t1:
        return 0

    _xa0, _ya0, _ta0 = _get_point(seg_a, (_t0 - ta0) / (ta1 - ta0))
    _xa1, _ya1, _ta1 = _get_point(seg_a, (_t1 - ta0) / (ta1 - ta0))
    _xb0, _yb0, _tb0 = _get_point(seg_b, (_t0 - tb0) / (tb1 - tb0))
    _xb1, _yb1, _tb1 = _get_point(seg_b, (_t1 - tb0) / (tb1 - tb0))

    d1 = haversine(_xa0, _ya0, _xb0, _yb0)
    d2 = haversine(_xa1, _ya1, _xb1, _yb1)
    if d1 <= distance_threshold:
        if in_box(_xb0, _yb0, _tb0, box_id):
            d = haversine(_xa0, _ya0, _xa1, _ya1)
            return (_distance_func(d1) + _distance_func(d2)) * d / 2
        else:
            return 0

    if d2 <= distance_threshold:
        if in_box(_xb1, _yb1, _tb1, box_id):
            d = haversine(_xa0, _ya0, _xa1, _ya1)
            return (_distance_func(d1) + _distance_func(d2)) * d / 2
        else:
            return 0

    return 0


def in_box(x, y, t, box_id):
    _x0 = math.floor((x - xmin) / wx)
    _y0 = math.floor((y - ymin) / wy)
    _t0 = math.floor((t - tmin) / wt)
    return (_x0, _y0, _t0) == box_id


def get_scores(joined_pair):
    """
    :param joined_pair:(box_id,([(tra_id,[xyt]),...],[(tra_id,[xyt])...]))
    :return:
    """
    boxid, pair = joined_pair
    mo_list, car_list = pair

    result = []

    for mo in mo_list:
        moid, moseg = mo
        for car in car_list:
            carid, carseg = car
            tmp_score = score(boxid, moseg, carseg)
            if tmp_score >= 0.0000001:
                result.append(((moid, carid), tmp_score))
    return result


def get_top(n, l):
    """

    :param n: top n
    :param l: [(score,id),...]
    :return:
    """
    return heapq.nlargest(n, l)


def foldf(l, pair):
    heapq.heappushpop(l, pair)
    return l


def combinef(l1, l2):
    return heapq.nlargest(20, l1 + l2)

# two sample datasets
tra_file1 = 'data/sample/data1.txt'
tra_file2 = 'data/sample/data2.txt'

if __name__ == "__main__":
    conf = SparkConf().setAppName('trajectory_matching').setMaster('local') \
        .set('spark.executor.memory', '12g') \
        .set('spark.driver.memory', '2g')
    sc = SparkContext(conf=conf)

    # [(id,seg),...]
    mo_rdd = sc.textFile(tra_file1). \
        map(line2pair)
    car_rdd = sc.textFile(tra_file2). \
        map(line2pair)

    mo_box_seg_rdd = mo_rdd.flatMap(flat_idseg_with_bounds)
    car_box_seg_rdd = car_rdd.flatMap(flat_idseg)

    grouped_mo_box_seg_rdd = mo_box_seg_rdd.groupByKey()
    grouped_car_box_seg_rdd = car_box_seg_rdd.groupByKey()

    joined_rdd = grouped_mo_box_seg_rdd.join(grouped_car_box_seg_rdd)

    scores_rdd = joined_rdd.flatMap(get_scores)
    idid_score_rdd = scores_rdd.reduceByKey(lambda a, b: a + b)

    id_scoreid_rdd = idid_score_rdd.map(lambda r: (r[0][0], (r[1], r[0][1])))

    topn_id_scoreid_rdd = id_scoreid_rdd.aggregateByKey([(0, 0)] * 20, foldf, combinef)
    ordered_id_scoreid_rdd = topn_id_scoreid_rdd.map(lambda pair: (pair[0], sorted(pair[1], reverse=True)))

    id_ids_rdd = ordered_id_scoreid_rdd.map(lambda pair: (pair[0], list(map(lambda p: p[1], pair[1]))))
    print(ordered_id_scoreid_rdd.collect())
    id_rank_rdd = id_ids_rdd.map(
        lambda pair: (pair[0], pair[1].index(pair[0] // 10000) if pair[0] // 10000 in pair[1] else -1))
    id_rank_rdd.cache()
    top1_rdd = id_rank_rdd.filter(lambda p: 0 <= p[1] < 1)
    top5_rdd = id_rank_rdd.filter(lambda p: 0 <= p[1] < 5)
    top10_rdd = id_rank_rdd.filter(lambda p: 0 <= p[1] < 10)
    total_count = id_rank_rdd.count()
    id_rank_rdd.unpersist()
    print(top1_rdd.count() / total_count,
          top5_rdd.count() / total_count,
          top10_rdd.count() / total_count)

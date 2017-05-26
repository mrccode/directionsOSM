#!/usr/bin/python
# ----------------------------------------------------------------
# Need to add quick description what it does
#
# ------------------------------------------------------
# Usage as library:
# TODO: How to use it
# ------------------------------------------------------
# Copyright 2017 - Anna Cibis, Marcin Pastecki
# The goal of this is to calculate distance between two
# locations using OSM data instead of Google Directions
# API.
# ------------------------------------------------------
from pyroutelib2.route import Router
from pyroutelib2.loadOsm import LoadOsm
import csv
import pandas as pd
import math
from multiprocessing import Process
from multiprocessing import Queue
import os

# Set line distance of an area within which walking distance should be calculated
# This should be in kilometers
within_distance_global = 1


# point1 and point2 are dictionaries wit two keys each:
# lon and lat
def distance_between_coordinates(point1, point2):
    point2['lat'] = float(point2['lat'])
    point2['lon'] = float(point2['lon'])
    point1['lat'] = float(point1['lat'])
    point1['lon'] = float(point1['lon'])
    ky = 40000 / 360
    kx = math.cos(math.pi * point2['lat'] / 180.0) * ky
    dx = math.fabs(point2['lon'] - point1['lon']) * kx
    dy = math.fabs(point2['lat'] - point1['lat']) * ky
    return dx * dx + dy * dy


def load_data(objectsfile, poifile):
    objects = pd.read_csv(filepath_or_buffer=objectsfile, sep=';')
    objects['closestStop'] = 0
    objects['closestStop'] = objects['closestStop'].asobject
    pois = pd.read_csv(filepath_or_buffer=poifile, sep=';')
    return objects, pois


# TODO: This should be done better. Sorting pois around object's
# TODO: coordinates and limiting them that way may be good idea.
# This will return list containing coordinates of
def find_closest_objects(object, pois, within_distance):
    nearpois = []
    centerPoint = {
        'lat': object['lat'],
        'lon': object['lon'],
    }
    for ind, row in pois.iterrows():
        checkPoint = {
            'lat': row['lat'],
            'lon': row['lon'],
        }
        if distance_between_coordinates(centerPoint, checkPoint) < within_distance:
            nearpois.append([checkPoint['lat'], checkPoint['lon']])
    return nearpois


#
# objects, pois are of dataframe type
def find_pois(objects, pois, within_distance, data, router, out_q):
    count = 0
    rows = []
    for ind, row in objects.iterrows():
        closest_objects = find_closest_objects(row, pois, within_distance)
        distances = []
        count += 1
        if count % 100 == 0:
            print(count)
        node1 = data.findNode(float(row['lat']), float(row['lon']))
        for object in closest_objects:
            if (float(row['lat']) == object[0] and float(row['lon']) == object[1]):
                result = 'success'
                routedistance = 0
            else:
                node2 = data.findNode(object[0], object[1])
                result, route, routedistance = router.doRoute(node1, node2)
            if result == 'success':
                print("Walking distance: %s" % routedistance)
                distances.append(routedistance)
            else:
                print("Failed (%s)" % result)
        print("Number of POIs: %i, closest POI: %s" % (len(distances), min(distances)))
        row['NumberOfPOIs'] = len(distances)
        row['DistanceToClosestPoi'] = min(distances)
        out_q.put(row)
        rows.append(row)
    return rows





# Two points:
# Katowice, Ziołowa 43 - 50.2105169,18.99566
# Katowice, Wesoła 7 - 50.214563, 18.99711
# According to Google Directions API,
# walking distance is 776 meters.

n1 = {
    'lat': 50.2105169,
    'lon': 18.99566
}

n2 = {
    'lat': 50.214563,
    'lon': 18.99711
}

# 50.229681, 19.060736
# 50.2263559, 19.064356
an1 = {
    'lat': 50.229681,
    'lon': 19.060736
}

an2 = {
    'lat': 50.2263559,
    'lon': 19.064356
}

# 50.2677029, 18.9833877
# 50.2661356, 18.986522
bn1 = {
    'lat': 50.2677029,
    'lon': 18.9833877
}

bn2 = {
    'lat': 50.2661356,
    'lon': 18.986522
}

cn1 = {
    'lat': 50.1980765,
    'lon': 18.983124
}

cn2 = {
    'lat': 50.1972386,
    'lon': 18.9815431
}
def testRun(n1, n2):
    # Test suite - do a little bit of easy routing in Katowice
    data = LoadOsm("foot")

    node1 = data.findNode(n1['lat'], n1['lon'])
    node2 = data.findNode(n2['lat'], n2['lon'])

    router = Router(data)

    print("Distance: %s" % (router.distance(node1, node2)))

    result, route, routedistance = router.doRoute(node1, node2)
    if result == 'success':
        print("Walking distance: %s" % routedistance)
    else:
        print("Failed (%s)" % result)


def reader(queue):
    # Read from the queue
    while True:
        msg = queue.get()
        print('msg: %s' % msg)
        if (msg == 'DONE'):
            break

def test(n, out_q):
    result = n*n*n
    out_q.put(result)


if __name__ == "__main__":
    objects, pois = load_data(objectsfile="/home/mapastec/Documents/studia/KoloNaukowe/dane/lokale-male.csv",
                              poifile="/home/mapastec/Documents/studia/KoloNaukowe/dane/szkolykur.csv")

    data = LoadOsm("foot")
    router = Router(data)
    #queue = Queue()

    usable_cpu_number = len(os.sched_getaffinity(0))
    out_q = Queue()
    procs = []

    reader_p = Process(target=reader, args=((out_q),))
    reader_p.daemon = True
    reader_p.start()

    print(len(objects))

    if len(objects) > usable_cpu_number:
        current_row = -1
        for i in range(usable_cpu_number):
            current_row += 1
            last_row = int(current_row + len(objects) / 4)
            current_row = last_row
            current_df = objects[current_row:last_row]
            p = Process(name='Process %d' % i, target=find_pois, args=(current_df, pois, 1, data, router, out_q,))
            #p = Process(name='Process %d' % i, target=test, args=(i,out_q,))
            procs.append(p)
            p.start()
            print(p.name)

    for p in procs:
        p.join()

    reader_p.join()

    #print("from q: %s" % out_q.get())
    #print(pois[10:20])
    #


    #p = Process(target=find_pois, args=(objects, pois, 1, data, router,))
    #p.start()
    #p.join()

    #result = find_pois(objects=objects, pois=pois, within_distance=1, data=data, router=router)

    #print(result)

    #queue.put('DONE')
    #reader(queue=queue)
    #reader_p.join()




    #testRun(n1, n2)
    #testRun(an1, an2)
    #testRun(bn1, bn2)
    #testRun(cn1, cn2)
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
from multiprocessing import Pool, Pipe, Process, Lock
import numpy as np
from functools import partial

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


def load_data(objectsfile, poifile, sep1, sep2):
    _objects = pd.read_csv(filepath_or_buffer=objectsfile, sep=sep1)
    _pois = pd.read_csv(filepath_or_buffer=poifile, sep=sep2)
    return _objects, _pois


# TODO: This should be done better. Sorting pois around object's
# TODO: coordinates and limiting them that way may be good idea.
# This will return list containing coordinates of
def find_closest_objects(centerPoint, pois, within_distance):
    nearpois = []
    for ind, row in pois.iterrows():
        checkPoint = {
            'lat': row['lat'],
            'lon': row['lon'],
        }
        if distance_between_coordinates(centerPoint, checkPoint) < within_distance:
            nearpois.append([checkPoint['lat'], checkPoint['lon']])
    return nearpois


#
# pois is dataframe
def find_pois(pois, within_distance, data, router, fprow, pipe_):
    centerpoint = {
        'lat': float(fprow['lat']),
        'lon': float(fprow['lon']),
    }
    closest_objects = find_closest_objects(centerpoint, pois, within_distance)
    distances = []
    node1 = data.findNode(float(centerpoint['lat']), float(centerpoint['lon']))
    for object_ in closest_objects:
        if float(centerpoint['lat']) == object_[0] and float(centerpoint['lon']) == object_[1]:
            foundroute = 'success'
            routedistance = 0
        else:
            node2 = data.findNode(object_[0], object_[1])
            foundroute, route, routedistance = router.doRoute(node1, node2)
        if foundroute == 'success':
            print("Walking distance: %s" % routedistance)
            distances.append(routedistance)
        else:
            print("Failed (%s)" % foundroute)
    if len(distances) > 0 and min(distances):
        fprow['NumberOfPOIs'] = len(distances)
        fprow['DistanceToClosesPoi'] = min(distances)
        pipe_.send(fprow)
        return "%s %s" % (len(distances), min(distances))
    else:
        return "0 666"


def add_distance(df_, pois_, within_distance_, pipe_):
    data_ = LoadOsm("foot")
    router_ = Router(data_)
    df_['NumberOfPOIsAndDistanceToClosestPoi'] = df_.apply(lambda row_:
                                                           find_pois(
                                                               pois=pois_,
                                                               within_distance=within_distance_,
                                                               data=data_,
                                                               router=router_,
                                                               fprow=row_,
                                                               pipe_=pipe_
                                                           ), axis=1)
    return df_


def parallelize_dataframe(df, func):
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def reader(pipeAndLock):
    output_p, input_p, lock = pipeAndLock
    input_p.close()
    rowscount = 0
    rows = []
    while True:
        try:
            msg = output_p.recv()
            rows.append(msg)
            rowscount += 1
            if rowscount == 100:
                lock.acquire()
                with open("output.csv", "a") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)
                    del rows[:]
                    rowscount = 0
                lock.release()
        except EOFError:
            writer = csv.writer(f)
            writer.writerows(rows)
            break


if __name__ == "__main__":

    num_partitions = 2  # number of partitions to split dataframe
    num_cores = 1  # number of cores on your machine

    folder = "/home/mapastec/Documents/studia/KoloNaukowe/dane/"

    objects, pois = load_data(objectsfile="%sremaster_lokale.csv" %folder,
                              poifile="%sszkolykur.csv" %folder,
                              sep1=',', sep2=';')
    within_distance = 1
#    data = LoadOsm("foot")
#    router = Router(data)
    output_p, input_p = Pipe()
    lock = Lock()

    reader_p = Process(target=reader, args=((output_p, input_p, lock),))
    reader_p.start()
    output_p.close()

    partialAddDistance = partial(add_distance, pois_=pois, within_distance_=within_distance,
                                 pipe_=input_p)

    outputdf = parallelize_dataframe(objects, partialAddDistance)

    print(outputdf.head())
    outputdf.to_csv('outputszkolydf.csv', sep=';')
    reader_p.join()

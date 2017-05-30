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
# objects, pois are of dataframe type
def find_pois(centerPoint, pois, within_distance, fprow, pipe_):
    closest_objects = find_closest_objects(centerPoint, pois, within_distance)
    distances = []
    data_ = LoadOsm("foot")
    router_ = Router(data_)
    node1 = data_.findNode(float(centerPoint['lat']), float(centerPoint['lon']))
    for object_ in closest_objects:
        if (float(centerPoint['lat']) == object_[0] and float(centerPoint['lon']) == object_[1]):
            foundroute = 'success'
            routedistance = 0
        else:
            node2 = data_.findNode(object_[0], object_[1])
            foundroute, route, routedistance = router_.doRoute(node1, node2)
        if foundroute == 'success':
            print("Walking distance: %s" % routedistance)
            distances.append(routedistance)
        else:
            print("Failed (%s)" % foundroute)
    if len(distances) > 0 and min(distances):
        fprow['NumberOfPOIs'] = len(distances)
        fprow['DistanceToClosesPoi'] = min(distances)
        pipe_.send(fprow)
        return (len(distances), min(distances))
    else:
        return (0, 666)


def add_distance(df):
    answer = df.apply(lambda row_:
                         {
                               find_pois(
                                   {
                                       'lat': row_['lat'],
                                       'lon': row_['lon'],
                                   },
                                   pois=pois,
                                   within_distance=within_distance,
                                   fprow=row_,
                                   pipe_=input_p
                               )
                         }, axis=1)
    if answer:
        df['NumberOfPOIsAndDistanceToClosestPoi'] = answer
    return df


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

    num_partitions = 10  # number of partitions to split dataframe
    num_cores = 4  # number of cores on your machine

    folder = "/home/mapastec/Documents/studia/KoloNaukowe/dane/"

    objects, pois = load_data(objectsfile="%sremaster_lokale.csv" %folder,
                              poifile="%sszkolykur.csv" %folder,
                              sep1=',', sep2=';')
    within_distance = 1
    output_p, input_p = Pipe()
    lock = Lock()

    reader_p = Process(target=reader, args=((output_p, input_p, lock),))
    reader_p.start()
    output_p.close()

    outputdf = parallelize_dataframe(objects, add_distance)

    print(outputdf.head())
    reader_p.join()
    outputdf.to_csv('outputdf.csv', sep=';')
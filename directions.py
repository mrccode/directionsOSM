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
    with open(objectsfile, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        objects = pd.DataFrame(list(reader))
    objects['closestStop'] = 0
    objects['closestStop'] = objects['closestStop'].asobject

    with open(poifile, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        pois = pd.DataFrame(list(reader))

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
            nearpois.append([pois['lat'], pois['lon']])
    return nearpois






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


if __name__ == "__main__":
    testRun(n1, n2)
    testRun(an1, an2)
    testRun(bn1, bn2)

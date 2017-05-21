#!/usr/bin/python
#----------------------------------------------------------------
# Need to add quick description what it does
#
#------------------------------------------------------
# Usage as library:
# TODO: How to use it
#------------------------------------------------------
# Copyright 2017 - Anna Cibis, Marcin Pastecki
# The goal of this is to calculate distance between two
# locations using OSM data instead of Google Directions
# API.
#------------------------------------------------------
from pyroutelib2.route import Router
from pyroutelib2.loadOsm import LoadOsm
import csv
import pandas as pd


def load_data(localefile, poifile):
    with open(localefile, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        locale = pd.DataFrame(list(reader))
    locale['closestStop'] = 0
    locale['closestStop'] = locale['closestStop'].asobject

    with open(poifile, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        pois = pd.DataFrame(list(reader))

    return locale, pois

def find_closest_objects(localefile, poifile):






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

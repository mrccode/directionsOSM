#!/usr/bin/python
#----------------------------------------------------------------
# Routing for OSM data
#
#------------------------------------------------------
# Usage as library:
#   datastore = loadOsm('transport type')
#   router = Router(datastore)
#   result, route = router.doRoute(node1, node2)
#
# (where transport is cycle, foot, car, etc...)
#------------------------------------------------------
# Copyright 2007-2008, Oliver White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# mrc and ymra modified
#
# doRoute function has been modified so that it returns the route distance
#
# distance function has been modified so that the distance
# is calculated with better precision
#------------------------------------------------------
import math

from pyroutelib2.loadOsm import LoadOsm


class Router(object):
  def __init__(self, data):
    self.data = data

  def distance(self, n1, n2):
    """Calculate distance between two nodes"""
    lat1 = self.data.rnodes[n1][0]
    lon1 = self.data.rnodes[n1][1]
    lat2 = self.data.rnodes[n2][0]
    lon2 = self.data.rnodes[n2][1]
    # TODO: projection issues
    # mrc: added better distance calculation
    # as made by ymra
    ky = 40000 / 360
    kx = math.cos(math.pi * lat1 / 180.0) * ky
    dx = math.fabs(lon1 - lon2) * kx
    dy = math.fabs(lat1 - lat2) * ky
    dist = math.sqrt(dx * dx + dy * dy)
    #dist = math.sqrt(math.pow(dx, 2) - math.pow(dy, 2))
    return(dist)

  def doRoute(self,start,end):
    """Do the routing"""
    self.searchEnd = end
    closed = [start]
    self.queue = []
    
    # Start by queueing all outbound links from the start node
    blankQueueItem = {'end':-1,'distance':0,'nodes':str(start)}

    try:
      for i, weight in self.data.routing[start].items():
        self.addToQueue(start,i, blankQueueItem, weight)
    except KeyError:
      return('no_such_node',[],None)

    # Limit for how long it will search
    count = 0
    # modified from 1000000 to 20000
    while count < 20000:
      count = count + 1
      try:
        nextItem = self.queue.pop(0)
      except IndexError:
        # Queue is empty: failed
        # TODO: return partial route?
        return('no_route',[],None)
      x = nextItem['end']
      if x in closed:
        continue
      if x == end:
        # Found the end node - success
        routeNodes = [int(i) for i in nextItem['nodes'].split(",")]
        # mrc - adding code that will return the distance of the whole route.
        previousnode = None
        routedistance = 0
        for currentnode in routeNodes:
            if previousnode:
                routedistance += self.distance(previousnode, currentnode)
            previousnode = currentnode
        return('success', routeNodes, routedistance)
      closed.append(x)
      try:
        for i, weight in self.data.routing[x].items():
          if not i in closed:
            self.addToQueue(x,i,nextItem, weight)
      except KeyError:
        pass
    else:
      return('gave_up',[],None)
  
  def addToQueue(self,start,end, queueSoFar, weight = 1):
    """Add another potential route to the queue"""

    # getArea() checks that map data is available around the end-point,
    # and downloads it if necessary
    #
    # TODO: we could reduce downloads even more by only getting data around
    # the tip of the route, rather than around all nodes linked from the tip
    end_pos = self.data.rnodes[end]
    self.data.getArea(end_pos[0], end_pos[1])
    
    # If already in queue, ignore
    for test in self.queue:
      if test['end'] == end:
        return
    distance = self.distance(start, end)
    if(weight == 0):
      return
    distance = distance / weight
    
    # Create a hash for all the route's attributes
    distanceSoFar = queueSoFar['distance']
    queueItem = { \
      'distance': distanceSoFar + distance,
      'maxdistance': distanceSoFar + self.distance(end, self.searchEnd),
      'nodes': queueSoFar['nodes'] + "," + str(end),
      'end': end}
    
    # Try to insert, keeping the queue ordered by decreasing worst-case distance
    count = 0
    for test in self.queue:
      if test['maxdistance'] > queueItem['maxdistance']:
        self.queue.insert(count,queueItem)
        break
      count = count + 1
    else:
      self.queue.append(queueItem)

if __name__ == "__main__":
  # Test suite - do a little bit of easy routing in birmingham
  data = LoadOsm("cycle")

  node1 = data.findNode(52.552394,-1.818763)
  node2 = data.findNode(52.563368,-1.818291)

  print(node1)
  print(node2)

  router = Router(data)
  result, route = router.doRoute(node1, node2)
  if result == 'success':
    # list the nodes
    print(route)

    # list the lat/long
    for i in route:
      node = data.rnodes[i]
      print("%d: %f,%f" % (i,node[0],node[1]))
  else:
    print("Failed (%s)" % result)


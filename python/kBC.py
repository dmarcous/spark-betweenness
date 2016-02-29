import networkx as nx   
from collections import deque
import time
import sys
import os

print('Usage: python kBC.py filename k')
args = sys.argv
os.chdir(sys.path[0])
sg = nx.read_edgelist(args[1], nodetype=int)#, create_using = nx.DiGraph())
k = int(args[2])

inf = float('inf')
start = time.clock()


# Betweenness centrality
kBC = dict({key:0 for key in sg.nodes()})
for s in sg.nodes():
    #Structures
    Q = deque() #Queue
    S = deque() #Stack
    dist = dict({key:inf for key in sg.nodes()})
    sig = dict({key:float(0) for key in sg.nodes()})
    pred = dict({key:[] for key in sg.nodes()})
    
    #Initialize
    Q.append(s)
    dist[s] = 0
    sig[s] = 1
    
    #BFS - top-down
    while len(Q) > 0:
        v = Q.popleft()
        if dist[v] > k:
            break;
        S.append(v)
        for w in sg[v]:
            if dist[w] == inf:
                dist[w] = dist[v] + 1
                if dist[w] <= k:
                    Q.append(w)
            if dist[w] == dist[v] + 1:
                sig[w] += sig[v]
                pred[w].append(v)
    #Aggregate - bottom-up
    delta = dict({key:float(0) for key in sg.nodes()})
    while len(S) > 0:
        v = S.pop()
        for w in pred[v]:
            delta[w] += (delta[v]+1) * sig[w] / sig[v]
        if v!=s:
            if(sg.is_directed()):
                kBC[v] += delta[v]
            else: kBC[v] += delta[v] / 2.0
end = time.clock()
print('My time: ' + str(end - start))


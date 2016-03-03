#!/usr/bin/python
graph = {} # Graph is a dictionary to hold our child-parent relationships.
inodes = {}
with open('dirInodeMap-New.csv','r') as f:
    for row in f:
        child, parent = row.split('\t')
        graph.setdefault(parent.replace('\n', ''), []).append(child)
with open('inode2Name.csv','r') as f2:
    for row in f2:
        inode, name = row.split(', ')
        inodes.setdefault(inode, []).append(name.replace('\n',''))
def find_all_paths(graph, start, end, path=[]):
    path = path + inodes[start]
    print start, '/'.join(path)[1:]
    #if start == end:
    #    return [path]

    if not graph.has_key(start):
        return []

    paths = []

    for node in graph[start]:
        if node not in path:
            newpaths = find_all_paths(graph, node, end, path)
            for newpath in newpaths:
                paths.append(newpath)
    return paths

find_all_paths(graph, '16385' ,'1419711')

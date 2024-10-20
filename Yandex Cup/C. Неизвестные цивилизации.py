import os
import networkx as nx

with open("./persons.txt", 'r') as f:
    persons = f.read().split("\n")

with open("./pairs.txt", 'r') as f:
    pairs = f.read().split("\n")
    pairs = list(map(lambda item: item.split(" - "), pairs))


G = nx.Graph()

for person in persons:
    G.add_node(person)

for person1, person2 in pairs:
    G.add_edge(person1, person2)

connected_components = list(nx.connected_components(G))

print("Tsivilizatsiyalar soni:", len(connected_components))

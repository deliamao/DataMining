# below using ppt suggestion count betweenness method
import sys
import networkx as nx
import community
import numpy as np
import matplotlib.pyplot as plt

# ________the main funtion _________________
def main(inputfile):
    # build a new graph
    new_graph = build_graph(inputfile)
    # get the betweenness :
    # betweenness = get_betweenness(new_graph)
    comp_num = nx.number_connected_components(new_graph)
    new_graph_comp = list(nx.connected_components(new_graph))
    origin_comp_dict = {}
    origin_num = 0
    for single_comp in new_graph_comp:
        for each_node in single_comp:
            origin_comp_dict[each_node] = origin_num
        origin_num += 1
    max = community.modularity(origin_comp_dict, new_graph)
    rst = origin_comp_dict
    best_compn_num = comp_num

    # caculate the modulatiry
    temp = new_graph.copy()
    i = 0
    while comp_num < len(new_graph.nodes()):
        max_edge = 0.0
        betweenness = get_betweenness(temp)
        for k, v in betweenness.items():
            if v > max_edge:
                max_edge = v
        if (max_edge != 0.0):
            for k, v in betweenness.items():
                if v == max_edge:
                    temp.remove_edge(k[0], k[1])
            currend_comp_num = nx.number_connected_components(temp)
            componenents = list(nx.connected_components(temp))
            currend_comp_dict = {}
            num = 0
            for single_comp in componenents:
                for each_node in single_comp:
                    currend_comp_dict[each_node] = num
                num += 1
            current_mod = community.modularity(currend_comp_dict, new_graph)
            if current_mod >= max:
                max = current_mod
                rst = currend_comp_dict
                best_compn_num = currend_comp_num
            comp_num = currend_comp_num
            i += 1
    result_list = []
    for i in range(best_compn_num):
        result_list.append([])
    for key, value in rst.items():
        result_list[value].append(key)
    for compn in result_list:
        print sorted(compn)
    value = [rst.get(node) for node in new_graph.nodes()]
    draw_graph(new_graph, value)

# ________the draw graph _________________
def draw_graph(graph, values):
    nx.draw_networkx(graph, node_color=values)
    plt.axis('off')
    plt.savefig(sys.argv[2])
    plt.show()

# ________caculate the betweeness  using the follow three function_________________
def update_betweens(leaf, tmp_betweenness, node_visted, node_parents, score):
    if len(node_parents[leaf]) > 0:
        each_parent_score = score / len(node_parents[leaf])
        for parent in node_parents[leaf]:
            if leaf < parent:
                tmp_betweenness[(leaf, parent)] += each_parent_score
            else:
                tmp_betweenness[(parent, leaf)] += each_parent_score
            if node_visted[parent] == 1:
                update_betweens(parent, tmp_betweenness, node_visted, node_parents, each_parent_score)
            elif node_visted[parent] == 0:
                node_visted[parent] = 1
                update_betweens(parent, tmp_betweenness, node_visted, node_parents, each_parent_score + 1.0)

# ________BFS Graph_________________
def shortest_path(node, new_graph, edges_point):
    tmp_betweenness = {}
    for edge in new_graph.edges():
        tmp_betweenness[edge] = 0.0
    leaves = []
    node_parents = {}
    queue = []
    queue.append(node)
    node_depth = {}
    node_parents[node] = []
    node_depth[node] = 0
    node_visted = {}
    for node in new_graph.nodes():
        node_visted[node] = 0
    while len(queue) != 0:
        current_node = queue.pop(0)
        # print(current_node)
        if edges_point.has_key(current_node):
            candidate_children = edges_point[current_node]
            children_number = 0
            for child in candidate_children:
                # blank new child
                if child not in node_depth:
                    node_parents[child] = [current_node]
                    queue.append(child)
                    children_number += 1
                    node_depth[child] = node_depth[current_node] + 1
                else:
                    if node_depth[child] > node_depth[current_node]:
                        # it is a child already been add to queue,only need to udate parent
                        node_parents[child].append(current_node)
                        children_number += 1
            # if children_number  = 0
            # it means it is the leave
            if children_number == 0:
                leaves.append(current_node)
    # print(leaves)
    for leaf in leaves:
        node_visted[leaf] = 1
        update_betweens(leaf, tmp_betweenness, node_visted, node_parents, 1.0)
    return tmp_betweenness

# ________get the betweenness and call BFS and Update betweenn_________________
def get_betweenness(new_graph):
    betweenness = {}
    edges_point = {}
    # initial betweenness
    for edge in new_graph.edges():
        betweenness[edge] = 0.0
    for edge in new_graph.edges():
        if edge[0] in edges_point.keys():
            edges_point[edge[0]].append(edge[1])

        else:
            edges_point[edge[0]] = [edge[1]]
        if edge[1] in edges_point.keys():
            edges_point[edge[1]].append(edge[0])
        else:
            edges_point[edge[1]] = [edge[0]]
    for node in new_graph.nodes():
        temp_betweenness = shortest_path(node, new_graph, edges_point)
        for key, value in betweenness.items():
            betweenness[key] += temp_betweenness[key]
    for key, value in betweenness.items():
        betweenness[key] = betweenness[key] * 0.5
    return betweenness


# ________build a graph _________________
def build_graph(inputfile):
    edge_list = []
    file = open(inputfile, 'r')
    for line in file:
        line = line.strip("\n")
        line = line.split(' ')
        # input file
        line[0] = int(line[0])
        line[1] = int(line[1])
        # just in case
        if line[0] > line[1]:
            tmp = line[0]
            line[0] = line[1]
            line[1] = tmp
        # line = line.split(' ')
        edge_list.append(line)
    graph = nx.Graph()
    # edge
    graph.add_edges_from(edge_list)
    return graph

# ________initia _________________
inputfile = sys.argv[1]
main(inputfile)

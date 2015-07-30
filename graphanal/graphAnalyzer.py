#!/usr/bin/env python

import networkx as nx
import os 
import json 
import matplotlib
matplotlib.use('Agg')


#better do a tree walk
def getMatchedBills(matchDirPath):
   matchedBills = list() 
   labels = ['modelBill','stateBill','matchPrecent','modelBillContent','stateBillContent']
   walkOpj =  os.walk(matchDirPath)
   for (dirpath, dirnames, filenames) in walkOpj:
        #logger.info( "model working in dirpath:%s" % dirpath)
        for fname in filenames:
            if "part" not in fname: continue
            ff = os.path.join(dirpath,fname)
            for line in open(ff).readlines():
                if line.strip(): matchedBills.append(dict(zip(labels, line.split('^^^'))))

   return matchedBills

def graphStats(G,detailed=False):
    # Can probably do more https://networkx.github.io/documentation/latest/reference/functions.html
    print "-------------------------------------"
    print "Number of edges in the graph: ", G.number_of_edges()
    print "Number of nodes in the graph: ", G.number_of_nodes()
    print "-------------------------------------"
    if detailed:
        print "Detailed graph statistics:" 
        for line in nx.generate_edgelist(G):
            print(line)

def fillGraph(matchedBills,keep_top_n=9999999):
    Gmatches = nx.Graph()
    #Read the dataset
    for mb in matchedBills:
        #load all nodes/edges into graph 
        if mb['modelBill'] != mb['stateBill']:
            label1 = mb['modelBill'].split("/")[-1]
            label2 = mb['stateBill'].split("/")[-1]
            #use inverse similarity as weight
            Gmatches.add_edge(label1,label2,weight=1./float(mb['matchPrecent']))

    if keep_top_n!=9999999: 
        for gnode in Gmatches.nodes():
            sorted_edges = sorted(Gmatches.edges(nbunch=[gnode],data=True), key=lambda (source,target,data): data['weight'],reverse=True)
            subset_to_remove = tuple(sorted_edges[:-keep_top_n])
            #print len(sorted_edges)
            #print "Remove those ", sorted_edges[:-keep_top_n]
            #print "Keep those ", sorted_edges[-keep_top_n:]
            for tuple_to_remove in subset_to_remove:
                Gmatches.remove_edge(*tuple_to_remove[:2])

    graphStats(Gmatches)

    return Gmatches


def pageRankDumpReport(G):
    #PageRank
    #alpha : float, optional
    #Damping parameter for PageRank, default=0.85.
    try:
        pr = nx.pagerank(G, alpha=0.9)
        json.dump(pr, open("graph_pagerank.json",'w'))
        print "Saving PageRank report to: ", "graph_pagerank.json"
    except: print "Too many edges got removed. Graph is disjoint... Adjust parameters for fillGraph"


def saveGraphJSON(G):
    from networkx.readwrite import json_graph
    for n in G:
        G.node[n]['name'] = n
    # write json formatted data
    d = json_graph.node_link_data(G) # node-link format to serialize
    # write json
    print "Saving graph to: ", "graph.json"
    json.dump(d, open("graph.json",'w'))


def displayGraphQuickly(G,plot_name="weighted_graph.png"):
    G2 = G.copy()

    elarge=[(u,v) for (u,v,d) in G2.edges(data=True) if d['weight'] >0.1]
    esmall=[(u,v) for (u,v,d) in G2.edges(data=True) if d['weight'] <=0.1]

    pos=nx.spring_layout(G2) # positions for all nodes

    # nodes
    nx.draw_networkx_nodes(G2,pos,node_size=700)

    # edges
    nx.draw_networkx_edges(G2,pos,edgelist=elarge,
                    width=6)
    nx.draw_networkx_edges(G2,pos,edgelist=esmall,
                    width=6,alpha=0.5,edge_color='r',style='dashed')

    # labels
    nx.draw_networkx_labels(G2,pos,font_size=9,font_family='sans-serif')

    matplotlib.pyplot.axis('off')
    matplotlib.pyplot.savefig(plot_name) # save as png

#Slightly modified Networkx's dfs_tree method
def dfs_custom_tree(G, source=None):
    T = nx.Graph()
    if source is None:
        T.add_nodes_from(G)
    else: 
        T.add_node(source)
    #print "All those edges"
    #for e in nx.dfs_edges(G,source):
    #    print e
    T.add_edges_from(nx.dfs_edges(G,source))
    #FIXME assign weights back to the graph T
    for u,v,g in G.edges(data=True):
        try:
            T[u][v]['weight'] = G[u][v]['weight']
        except: continue
    return T

#Mostly to preserve the weighgts
def bfs_custom_tree(G, reverse=False):
    T = nx.Graph()
    #in some sense, picking a random node...
    source = G.nodes()[0]
    T.add_node(source)
    T.add_edges_from(nx.bfs_edges(G,source,reverse=reverse))
    #FIXME assign weights back to the graph T
    for u,v,g in G.edges(data=True):
        try:
            T[u][v]['weight'] = G[u][v]['weight']
        except: continue
    return T


def dfsTraverse(G,doPreorder=True):
    if doPreorder: return nx.dfs_preorder_nodes(G)   
    else: return nx.dfs_postorder_nodes(G)

def dfsDescribe(G,edgesOnly=True):
    G2 = G.copy()
    if edgesOnly: return nx.dfs_edges(G2)
    else: return dfs_custom_tree(G2) #,'EO165_Issued.txt')

def bfsDescribe(G,edgesOnly=True):
    G2 = G.copy()
    if edgesOnly: return nx.bfs_edges(G2)
    else: return bfs_custom_tree(G2) #,'EO165_Issued.txt')


def dijkstraDumpReport(G,printOnScreen=False):
    paths = nx.all_pairs_dijkstra_path_length(G, weight='weight')
    json.dump(paths, open("graph_dijkstra.json",'w'))
    print "Saving all the Dijkstra lengths to: ", "graph_dijkstra.json"
    if printOnScreen:
        for path in paths.keys():
            print "------------------------------------------"
            print path, "  ", paths[path]
            print "\n"

if __name__ == '__main__':

    #Read the dataset
    matchedBills = getMatchedBills('./matches')   
    Gmatches = fillGraph(matchedBills) #,3)   

    #get stats about the newly filled graph
    graphStats(Gmatches)

    #dump graph to JSOn as a node-link structure
    saveGraphJSON(Gmatches) #provide argument to specify in what format do you want it saved
    #calculate pagerank
    pageRankDumpReport(Gmatches)

    #Djikstra
    dijkstraDumpReport(Gmatches) 

    #display the graph
    displayGraphQuickly(Gmatches)   

    #show the DFS tree structure
    Gdfs = dfsDescribe(Gmatches,False)
    graphStats(Gdfs)
    displayGraphQuickly(Gdfs,"weighted_graph_dfs.png") 

    #Gbfs = bfsDescribe(Gmatches,False)
    #graphStats(Gbfs)
    #displayGraphQuickly(Gbfs,"weighted_graph_bfs.png")

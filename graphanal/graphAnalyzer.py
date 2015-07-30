#!/us/bin/env python

import networkx as nx
import os 
import json 
import matplotlib
import operator
matplotlib.use('Agg')

graphlab_is_installed = True

try:
    from graphlab import SGraph, Vertex, Edge, pagerank
except: graphlab_is_installed = False
    

#Following loads all the bill pairs having similarity greater than 90% into the list   
def getMatchedBills(matchDirPath,ncount=-1):
   #Parameters: 
   #matchDirPath - path to directory with files (absolute path, does not matter if on HDFS or local)  
   #ncount - allows to control how many bills to keep, keep all if "-1". Useful if you have very larger dataset
   matchedBills = list() 
   labels = ['modelBill','stateBill','matchPrecent','modelBillContent','stateBillContent']
   walkOpj =  os.walk(matchDirPath)
   threshold = 0
   for (dirpath, dirnames, filenames) in walkOpj:
        for fname in filenames:
            if "part" not in fname: continue
            #Keep only top ncount bills 
            if threshold > ncount and ncount > 0: continue     
            threshold += 1    
            ff = os.path.join(dirpath,fname)
            for line in open(ff).readlines():
                if line.strip(): matchedBills.append(dict(zip(labels, line.split('^^^'))))

   return matchedBills

#Print statistics about the graph to the screen. Feel free to add more from the page linked
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
    #In case of SGraph print Gmatches.get_edges().print_rows(num_rows=40, num_columns=3)         
            
#Fill edges and nodes into the Networkx Graph data structure
def fillGraph(matchedBills,pr_preselect,keep_top_n=9999999):
    #pr_preselect - possibility to only select nodes with PageRank greater than a threshold: pr_preselect is a float threshold
    #keep_top_n - keep highest similarity n edges (lowest weight). Messes up PageRank though...
    Gmatches = nx.Graph()
    #Read the dataset
    for mb in matchedBills:
        #load all nodes/edges into graph 
        if mb['modelBill'] != mb['stateBill']:
            state1 = mb['modelBill'].split("/")[-4]
            year1 = mb['modelBill'].split("/")[-3] 
            state2 = mb['stateBill'].split("/")[-4]
            year2 = mb['stateBill'].split("/")[-3]   
            label1 = state1+year1+"_"+mb['modelBill'].split("/")[-1].rstrip(".txt")
            label2 = state2+year2+"_"+mb['stateBill'].split("/")[-1].rstrip(".txt")
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
         
    if pr_preselect > 0: 
        pr = nx.pagerank(Gmatches, alpha=0.9)
        for gnode in Gmatches.nodes():
            subset_to_remove = tuple(Gmatches.edges(nbunch=[gnode],data=True))
            if pr[gnode] < pr_preselect:
                for tuple_to_remove in subset_to_remove:
                    Gmatches.remove_edge(*tuple_to_remove[:2])
                    #Gmatches.remove_node(gnode)
                    
        #remove zero degree nodes
        to_remove = [node for node,degree in Gmatches.degree().items() if degree < 2]
        Gmatches.remove_nodes_from(to_remove)
                
        graphStats(Gmatches)

    return Gmatches

#Same as previous, but when we use graphlab
def fillSGraph(matchedBills):
    if not graphlab_is_installed: print "GraphLab is not installed!"
    Gmatches = None    
    try:
        Gmatches = SGraph()
        #Read the dataset
        weights = list()
        for mb in matchedBills:
            #load all nodes/edges into graph 
            if mb['modelBill'] != mb['stateBill']:
                label1 = mb['modelBill'].split("/")[-1]
                label2 = mb['stateBill'].split("/")[-1]
                #use inverse similarity as weight
                #Gmatches.add_edge(label1,label2,weight=1./float(mb['matchPrecent']))
                vertices = list()
                vertices.append(Vertex(label1))
                vertices.append(Vertex(label2))
                Gmatches = Gmatches.add_vertices(vertices)
                Gmatches = Gmatches.add_edges(Edge(label1,label2))
                weights.append(1./float(mb['matchPrecent']))
            
        Gmatches.edges['weight'] = weights  
    except: pass 
    
    return Gmatches    
    
#Calculate pagerank, dump to JSON
#Calculate cumulative pagerank per state     
def pageRankDumpReport(G,gtype="Networkx"):
    if gtype == "Networkx":
        #PageRank
        #alpha : float, optional
        #Damping parameter for PageRank, default=0.85.
        pr_perstate = dict()
        try:
            pr = nx.pagerank(G, alpha=0.9)
            json.dump(pr, open("graph_pagerank.json",'w'))
            print "Saving PageRank report to: ", "graph_pagerank.json"
            for node in pr.keys():
                k_str = node[:2]
                if k_str in pr_perstate.keys(): pr_perstate[k_str] += pr[node]
                else: pr_perstate[k_str] = pr[node]
            print "Cumulative pagerank per state: ", pr_perstate
        except: print "Too many edges got removed. Graph is disjoint... Adjust parameters for fillGraph"
    else: pass

#same as above, but only show on the screen            
def pageRankShowReport(G):
    #PageRank
    #alpha : float, optional
    #Damping parameter for PageRank, default=0.85.
    pr = nx.pagerank(G, alpha=0.9)
    sorted_pr = sorted(pr.items(), key=operator.itemgetter(1),reverse=True)
    print len(pr), " ", len(sorted_pr)
    for node,weight in sorted_pr:
        print "Next node: ", node, " has PR weight: ", weight
        print "++++++++++++++++++++++++"
        print "It has following links: "
        for from_node,to_node in G.edges(nbunch=[node]):
            print to_node

#Save graph to JSON
def saveGraphJSON(G):
    from networkx.readwrite import json_graph
    for n in G:
        G.node[n]['name'] = n
    # write json formatted data
    d = json_graph.node_link_data(G) # node-link format to serialize
    # write json
    print "Saving graph to: ", "graph.json"
    json.dump(d, open("graph.json",'w'))

#Get list of nodes with pagerank above the threshold
#For Grapglab only
def getToHighlight(G):
    if not graphlab_is_installed: print "GraphLab is not installed!"
    important = None    
    try:
        print "Highlights nodes, for SGraph only!"
        pr = pagerank.create(G)
        pr_out = pr['pagerank']     # SFrame
        #print pr_out['__id'] #['pagerank'] 
    
        important= set()
        for a,b in zip(pr_out['__id'],pr_out['pagerank']):
            if b > 0.2: important.add(a)  
            print b
    except: pass
    return important   
    
#Lightweiht graph visualization based on matplotlib
#Allows to save to png, switch between Networkx and Sgraph, coloring of edges by weight
def displayGraphQuickly(G,coloring="by_weight",plot_name="weighted_graph.png",gtype="Networkx"):

    if gtype == "Networkx":     
        G2 = G.copy()

        elarge=[(u,v) for (u,v,d) in G2.edges(data=True) if d['weight'] >0.1]
        esmall=[(u,v) for (u,v,d) in G2.edges(data=True) if d['weight'] <=0.1]
        
        #FIXME
        if coloring != "by_weight": 
            pass
            #pr = nx.pagerank(G2, alpha=0.9)
            #sorted_pr = sorted(pr.items(), key=operator.itemgetter(1),reverse=True)
            #elarge=[(node,weight) for node,weight in sorted_pr if weight >0.1]
            #esmall=[(u,v) for (u,v,d) in G2.edges(data=True) if d['weight'] <=0.1]

        pos=nx.spring_layout(G2) # positions for all nodes

        # nodes
        nx.draw_networkx_nodes(G2,pos,node_size=700)

        # edges
        nx.draw_networkx_edges(G2,pos,edgelist=elarge,
                width=2)
        nx.draw_networkx_edges(G2,pos,edgelist=esmall,
                width=2,alpha=0.5,edge_color='r',style='dashed')

        # labels
        nx.draw_networkx_labels(G2,pos,font_size=9,font_family='sans-serif')

        matplotlib.pyplot.axis('off')
        matplotlib.pyplot.savefig(plot_name) # save as png
    else:
        try:
            important = list(getToHighlight(Gmatches))
            Gmatches.show(vlabel='id', highlight=important, elabel='weight', elabel_hover=True)
        except: pass

#Calculate Dijkstra lengths against all of the nodes
#Possibly printo  on the screenor dump to JSON
def dijkstraDumpReport(G,printOnScreen=False):
    paths = nx.all_pairs_dijkstra_path_length(G, weight='weight')
    print type(paths)
    json.dump(paths, open("graph_dijkstra.json",'w'))
    print "Saving all the Dijkstra lengths to: ", "graph_dijkstra.json"
    if printOnScreen:
        for path in paths.keys():
            print "------------------------------------------"
            print path, "  ", paths[path]
            print "\n"        
        
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


###
#PLAY WITH THE CODE BELOW
###   


#Read the dataset
matchedBills = getMatchedBills('/Users/asvyatko/Desktop/Princeton/Spark_interactive/matches_3states90/',100)   
#Specify the PageRang threshold 
Gmatches = fillGraph(matchedBills, 0.02) #,3)   

#dump graph to JSON as a node-link structure
saveGraphJSON(Gmatches) #provide argument to specify in what format do you want it saved
#calculate pagerank
pageRankDumpReport(Gmatches,"Networkx")
#pageRankShowReport(Gmatches)

#Djikstra
dijkstraDumpReport(Gmatches,False) 

#display the graph
displayGraphQuickly(Gmatches,"by_weight","weighted_graph.png","Networkx")   

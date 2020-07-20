import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

def calcPR(nodes, rank):
    #sets variable to the total number of nodes adjacent 
    numNodes = len(nodes)

    #dividing the rank of that given iteration between the number of adjacent nodes
    splitRank = rank / numNodes

    #for every node adjacent, it will yield a key/value pair where the key is the node and the value is the divided rank.
    for node in nodes:
        yield (node, splitRank)


conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Load the adjacency list file
AdjList1 = sc.textFile("/home/mdo8/Assignment4/02AdjacencyList.txt")

#[u'1 2', u'2 3 4', u'3 4', u'4 1 5', u'5 3']
print(AdjList1.collect())

#splitting line by spaces
AdjList2 = AdjList1.map(lambda line : line.split(" "))  # 1. Replace the lambda function with yours

#split each line where the first element is the node and  the second element is a list of adjacent nodes 
AdjList3 = AdjList2.map(lambda x : (x[0], (x[1:])))  # 2. Replace the lambda function with yours

AdjList3.persist()
print(AdjList3.collect())

nNumOfNodes = AdjList3.count()
print("Total Number of nodes")
print(nNumOfNodes)

# Initialize each page's rank; since we use mapValues, the resulting RDD will have the same partitioner as links
print("Initialization")

#setting the inital page rank values for each node, for every node, the initial value is set to 0.2
PageRankValues = AdjList3.mapValues(lambda v : 0.2)  # 3. Replace the lambda function with yours
print(PageRankValues.collect())

# Run 30 iterations
print("Run 30 Iterations")
for i in range(1, 30):
    print("Number of Iterations")
    print(i)
    JoinRDD = AdjList3.join(PageRankValues)
    print("join results")
    print(JoinRDD.collect())

    #contributions = JoinRDD.flatMap(lambda (x, (y, z)) : x)  # 4. Replace the lambda function with yours
    #tuple unpacking no longer available.
    contributions = JoinRDD.flatMap(lambda node_anode_prank: calcPR(node_anode_prank[1][0], node_anode_prank[1][1]))
    print("contributions")
    print(contributions.collect())
    
    #adding node values according to key
    accumulations = contributions.reduceByKey(lambda x, y : x + y)  # 5. Replace the lambda function with yours
    print("accumulations")
    print(accumulations.collect())

    #calculating new page rank value using formula and sorting them by the key
    PageRankValues = accumulations.mapValues(lambda v : (v *.85) + (.15 / nNumOfNodes)).sortBy(lambda node : node[0])  # 6. Replace the lambda function with yours
    print("PageRankValues")
    print(PageRankValues.collect())

print("=== Final PageRankValues ===")
print(PageRankValues.collect())

# Write out the final ranks
PageRankValues.coalesce(1).saveAsTextFile("/home/mdo8/Assignment4/PageRankValues_Final")

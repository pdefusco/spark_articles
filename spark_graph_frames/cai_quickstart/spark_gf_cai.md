# Getting Started with Spark GraphFrames in Cloudera AI Workbench

Spark GraphFrames is a package within Apache Spark that allows users to perform graph processing operations on data using a DataFrame-based approach. It allows you to perform various graph operations like finding connected components, calculating shortest paths, identifying triangles, and more. Real world applications include  social network analysis, recommendation systems, web link structures, flight connections, and other scenarios where relationships between data points are crucial.

Cloudera AI (CAI) is a cloud-native service within the Cloudera Data Platform (CDP) that enables enterprise data science teams to collaborate across the full data lifecycle. It provides immediate access to enterprise data pipelines, scalable compute resources, and preferred tools, streamlining the process of moving analytic workloads from research to production.

Using Spark GraphFrames in Cloudera AI requires minimum configuration effort. This quickstart provides a basic example so you can get started in no time.

You can find the same content [as an article in the Cloudera Community](https://community.cloudera.com/t5/Community-Articles/Getting-Started-with-Spark-GraphFrames-in-Cloudera-AI/ta-p/399716).

### Requirements

You can use Spark GraphFrames in CAI Workbench with Spark Runtime Addon versions 3.2 or above. When you create the SparkSession object, make sure to select the right version of the package reflecting the Spark Runtime Addon you set during CAI Session launch from [SparkPackages](https://spark-packages.org/package/graphframes/graphframes).

This example was created with the following platform and system versions, but it will also work in other Workbench versions (below or above 2.0.46).

* CAI Workbench (a.k.a. "CML Workspace") on version 2.0.46.
* Spark Runtime Addon version 3.5.

### Steps to Reproduce the Quickstart

Crate a CAI Workbench Session with the following settings:
* Resource Profile of 2 vCPU, 8 GiB Mem, 0 GPU
* Editor: PBJ Workbench, Python 3.10 Kernel, Standard Edition, 2024.10 Version.

In the Session, run the script. Notice the following:

1. Line 46: the SparkSession object is created. The packages option is used to download the lib. You have to change the argument with the package version compatible to your Spark Runtime Addon version, if not using Spark 3.5, as listed in [SparkPackages](https://spark-packages.org/package/graphframes/graphframes).

```
spark = SparkSession.builder.appName("MyApp") \
            .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
            .getOrCreate()
```

2. Line 71: A GraphFrame object is instantiated using the two PySpark Dataframes.

```
from graphframes import *

# Vertex DataFrame
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edge DataFrame
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)
```

3. Lines 73 and below: You can now use GraphFrames methods to traverse and filter the graph based on relationships between data instances.

```
> g.vertices.show()
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  a|  Alice| 34|
|  b|    Bob| 36|
|  c|Charlie| 30|
|  d|  David| 29|
|  e| Esther| 32|
|  f|  Fanny| 36|
|  g|  Gabby| 60|
+---+-------+---+

> g.edges.show()
+---+---+------------+
|src|dst|relationship|
+---+---+------------+
|  a|  b|      friend|
|  b|  c|      follow|
|  c|  b|      follow|
|  f|  c|      follow|
|  e|  f|      follow|
|  e|  d|      friend|
|  d|  a|      friend|
|  a|  e|      friend|
+---+---+------------+

# Find the youngest user’s age in the graph. This queries the vertex DataFrame.

> g.vertices.groupBy().min("age").show()
+--------+
|min(age)|
+--------+
|      29|
+--------+
```

# Summary & Next Steps

Cloudera AI (CAI) is a cloud-native service within the Cloudera Data Platform (CDP) that enables enterprise data science teams to collaborate across the full data lifecycle. It provides immediate access to enterprise data pipelines, scalable compute resources, and preferred tools, streamlining the process of moving analytic workloads from research to production. In particular:

* A CAI Workbench Session allows you to directly access and analyze large datasets, run code (like Python or R), build and train machine learning models, using the flexibility of Spark Runtime Addons to deploy Spark on Kubernetes clusters with minumum configurations, with your Spark version of choice.

* The CAI Engineering team maintains and certifies Spark Runtime Addons so you don't have to worry about installing and configuring Spark on Kubernetes clusters when using CAI Workbench. You can check if your Spark version is supported [in the latest compatibilty matrix](https://docs.cloudera.com/machine-learning/cloud/release-notes/topics/ml-dl-compatibility.html?utm_source=chatgpt.com).

* The "packages" option can be used at SparkSession creation in order to download 3rd party packages such as GraphFrames. For more information this and other Spark Options, please refer to the official [Apache Spark Documentation](https://spark.apache.org/docs/latest/configuration.html).

* GraphFrames in particular allows you to query your Spark Dataframes in terms of relationships, thus empowering use cases ranging from Social Network analysis, to Recommendation Engines, and more. For more information, please refer to [the official documentation](https://graphframes.github.io/graphframes/docs/_site/index.html).

Finally, you can learn more about Cloudera AI Workbench with the following recommended blogs and community articles:

**[Cloudera AI - What You Should Know](https://community.cloudera.com/t5/Community-Articles/Cloudera-Machine-Learning-What-You-Should-Know/ta-p/292935?utm_source=chatgpt.com)**  
An insightful community article that provides an overview of CAI's features and capabilities, helping teams deploy machine learning workspaces that auto-scale and auto-suspend to save costs.

**[Illustrating AI/ML Model Development in Cloudera AI](https://medium.com/swlh/illustrating-ai-ml-model-development-in-cloudera-machine-learning-4ad7edcaa447)**  
A Medium tutorial that demonstrates how to create and deploy models using CML on the Cloudera Data Platform Private Cloud, offering practical insights into the model development process.

**[Cloudera Accelerators for ML Projects](https://cloudera.github.io/Applied-ML-Prototypes/?utm_source=chatgpt.com#/cloudera)**  
A catalog of Applied Machine Learning Prototypes (AMPs) that can be deployed with one click directly from CML, designed to jumpstart AI initiatives by providing tailored solutions for specific use cases.

**[Cloudera AI Overview](https://docs.cloudera.com/machine-learning/cloud/product/topics/ml-product-overview.html?utm_source=chatgpt.com)**  
Official documentation that offers a comprehensive overview of Cloudera AI, detailing its features, benefits, and how it facilitates collaborative machine learning at scale.

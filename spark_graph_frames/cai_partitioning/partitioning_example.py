#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul Michael de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.cores", 4) \
            .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
            .getOrCreate()

from graphframes import *

ageGroupDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.AgeGroup.csv")
caseDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Case.csv")
drugDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Drug.csv")
manufacturerDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Manufacturer.csv")
outcomeDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Outcome.csv")
reactionDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Reaction.csv")
reportSourceDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.ReportSource.csv")
therapyDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.nodes.Therapy.csv")
fallsUnderDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.FALLS_UNDER.csv")
hasReactionDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.HAS_REACTION.csv")
isConcomitantDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.IS_CONCOMITANT.csv")
isInteractingDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.IS_INTERACTING.csv")
isPrimarySuspectDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.IS_PRIMARY_SUSPECT.csv")
isSecondarySuspectDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.IS_SECONDARY_SUSPECT.csv")
isPrescribedSuspectDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.PRESCRIBED.csv")
receivedDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.RECEIVED.csv")
registeredDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.REGISTERED.csv")
reportedByDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.REPORTED_BY.csv")
resultedInDf = spark.read.option("delimiter", ",").option("header", True).csv("/home/cdsw/spark_graph_frames/cai_partitioning/data/healthcare-analytics.relationships.RESULTED_IN.csv")

ageGroupDf.show()
caseDf.show()
drugDf.show()
manufacturerDf.show()
outcomeDf.show()
reactionDf.show()
reportSourceDf.show()
therapyDf.show()
fallsUnderDf.show()
hasReactionDf.show()
isConcomitantDf.show()
isInteractingDf.show()
isPrimarySuspectDf.show()
isSecondarySuspectDf.show()
isPrescribedSuspectDf.show()
receivedDf.show()
registeredDf.show()
reportedByDf.show()
resultedInDf.show()

# Rename DF Columns



# Create a GraphFrame
g = GraphFrame(caseDf, ageGroupDf)
g.vertices.show()
g.edges.show()

# Get a DataFrame with columns "id" and "inDegree" (in-degree)
vertexInDegrees = g.inDegrees

# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
numFollows = g.edges.filter("relationship = 'follow'").count()

print(numFollows)

import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log
from pyspark.sql.types import StringType


k1 = 1.5
b = 0.75


if len(sys.argv) > 1:
    query = sys.argv[1]
else:
    query = input("Enter query: ")

query_terms = list(set(query.lower().split()))


spark = SparkSession.builder \
    .appName("BM25 Query") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.catalog.cassandracat", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

sc = spark.sparkContext


vocab_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="vocabulary", keyspace="search_index") \
    .load() \
    .filter(col("term").isin(query_terms))\
    .select("term", "document_frequency")


doc_stats_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="document_stats", keyspace="search_index") \
    .load() \
    .select("doc_id", "doc_length")


index_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="inverted_index", keyspace="search_index") \
    .load() \
    .filter(col("term").isin(query_terms)) \
    .select("term", "doc_id", "term_frequency")


total_docs = doc_stats_df.count()


avg_doc_len = doc_stats_df.agg({"doc_length": "avg"}).collect()[0][0]

idf_df = vocab_df.withColumn(
    "idf", log((total_docs - col("document_frequency") + 0.5) / (col("document_frequency") + 0.5) + 1.0)
)

index_with_idf = index_df.join(idf_df, "term")


full_index_df = index_with_idf.join(doc_stats_df, "doc_id")



def compute_bm25(row):
    term = row["term"]
    doc_id = row["doc_id"]
    tf = row["term_frequency"]
    doc_len = row["doc_length"]
    idf = row["idf"]


    score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_len / avg_doc_len))))
    return (doc_id, score)



bm25_scores = full_index_df.rdd.map(compute_bm25) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1]) \
    .take(10)


paths_rdd = sc.binaryFiles("hdfs:///data").keys()

doc_titles = paths_rdd.map(lambda path: path.split("/")[-1]) \
    .map(lambda filename: (filename.split("_")[0], "_".join(filename.split("_")[1:]).rsplit(".", 1)[0].replace("_", " ")))

doc_titles_dict = sc.broadcast(dict(doc_titles.collect()))




print("\nTop 10 relevant documents:\n")
for doc_id, score in bm25_scores:
    title = doc_titles_dict.value.get(doc_id, "Unknown Title")
    print(f"Doc ID: {doc_id} | Title: {title} | Score: {round(score, 3)}")


spark.stop()

# Interview Questions and Answers: Uber Data Analysis Notebook

This page summarizes key concepts from `BigData_UberDataAnalysis.ipynb` and provides sample interview questions with concise answers.

## 1. Spark Basics

**Q1.** *What is the purpose of a `SparkSession` and how is it related to `SparkContext`?*

A `SparkSession` is the entry point to programming with Spark in Python. It encapsulates the underlying `SparkContext` (which manages the connection to a Spark cluster) along with additional functionality for DataFrame and SQL operations. You typically create one `SparkSession` per application, and you can access its `SparkContext` via `spark.sparkContext`.

**Q2.** *How do you determine the number of partitions of an RDD, and why might you change it?*

You can check the number of partitions using `rdd.getNumPartitions()`. Adjusting partitions helps balance work across your cluster: increasing partitions can improve parallelism, while decreasing them can reduce overhead when dealing with small datasets.

## 2. RDD Transformations

**Q3.** *Explain the difference between `map`, `filter`, and `reduceByKey`.*

- `map` applies a function to each element of an RDD, returning a new RDD of the same length.
- `filter` keeps only elements that satisfy a predicate.
- `reduceByKey` combines values with the same key using a provided function, performing the reduction in a distributed manner.

**Q4.** *In the provided code, why is `reduceByKey` used to compute total trips per `(base + day)`?*

The notebook converts each record into a key/value pair `(base+day, trips)` and uses `reduceByKey` with the sum function to efficiently aggregate trip counts for each base and day combination across the cluster.

## 3. Data Parsing

**Q5.** *How can you parse date strings in PySpark and convert them to day-of-week values?*

The notebook parses date strings with Python's `datetime` module, e.g. `datetime.strptime(date_str, "%m/%d/%Y")`, then calls `weekday()` (or `strftime('%A')`) to obtain the day of the week. This can be done inside a `map` operation on an RDD.

## 4. Joins and Aggregations

**Q6.** *When might you use inner joins versus left/right/full outer joins on RDDs?*

- **Inner join** returns only keys present in both RDDs.
- **Left outer join** keeps all keys from the left RDD and matches from the right when available (missing values become `None`).
- **Right outer join** is the opposite: all keys from the right RDD are kept.
- **Full outer join** keeps all keys from both RDDs, filling missing values with `None`.

Choice depends on whether you need unmatched records from one or both RDDs.

**Q7.** *How does sorting (`sortBy`) work on an RDD, and what's the default sort order if none is specified?*

`sortBy` sorts elements by a specified key function. By default it sorts in ascending order. You can pass `ascending=False` to sort in descending order.

## 5. Optimization and Partitions

**Q8.** *What are some considerations in choosing an appropriate number of partitions for an RDD?*

Consider the size of your cluster, the dataset, and the balance between parallelism and task overhead. Too few partitions may underutilize resources, while too many may increase scheduling overhead. Spark's default partitioning can be tuned using `repartition()` or `coalesce()`.

**Q9.** *What are potential performance implications of using `reduceByKey` versus `groupByKey`?*

`reduceByKey` combines data locally on each partition before shuffling, reducing network I/O. `groupByKey` shuffles all values with the same key before aggregation, which can be inefficient and memory-intensive for large datasets.

## 6. PySpark vs. Other APIs

**Q10.** *What are the advantages and disadvantages of working with RDDs compared to using DataFrames or Datasets in Spark?*

- **Advantages of RDDs:** fine-grained control, type safety with custom objects, and the ability to use functional transformations directly.
- **Disadvantages:** less optimization than DataFrames, no automatic schema inference, and generally more verbose code. DataFrames offer Catalyst query optimization and better performance for structured data.

## 7. Troubleshooting

**Q11.** *If the dataset's header were not removed, how would that affect the result of aggregations?*

The header would be treated like a regular record, so the key derived from its fields would appear in the output and skew results. Typically, you filter out the header row before any aggregation.

**Q12.** *What are ways to debug or inspect the contents of an RDD?*

You can use actions like `take(n)` to view a few elements, `collect()` to retrieve all elements (carefully with large datasets), or `toDebugString()` to inspect the lineage of transformations.


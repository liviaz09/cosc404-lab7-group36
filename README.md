# COSC 404 - Database System Implementation<br/>Lab 7 - MapReduce using MongoDB

This lab practices programming with MapReduce using MongoDB.

## MapReduce on MongoDB (10 marks + 5 bonus marks)

[MapReduce](https://en.wikipedia.org/wiki/MapReduce) is a technique for processing large data sets involving two phases: a "map" phase that filters and distributes records based on key values and a "reduce" phase that produces a summary over all values that map to a given key.  MapReduce is designed for large, batch data set processing as both the "map" and 'reduce" phases can be parallelized across a cluster of machines.  Support for MapReduce is present in many systems including MongoDB.

There are a total of 6 Map-Reduce queries to write in the file `QueryMongoMapReduce.java`.  Test using the JUnit test file `TestMongoMapReduce.java`.

Note: MapReduce support is deprecated for MongoDB version 5 as MongoDB is moving to a different processing pipeline. However, MapReduce code still runs on MongoDB.

**Each MapReduce query is worth 2.5 marks.  Completing any 4 queries receives the full 10 marks for the assignment.  Complete all 6 for 15 marks (includes 5 bonus marks).**

### References

- [Apache Hadoop HDFS and MapReduce Implementation](https://hadoop.apache.org/)
- [MapReduce Wikipedia](https://en.wikipedia.org/wiki/MapReduce)
- [MongoDB MapReduce](https://docs.mongodb.org/manual/core/map-reduce/)
- [MongoDB MapReduce Examples](https://docs.mongodb.com/manual/tutorial/map-reduce-examples/)

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**

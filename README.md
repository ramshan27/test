# test
test
rdd1 = rdd1.filter( lambda rec : int(rec.split(",")[1]) == 2).map( lambda rec : (rec.split(",")[1],str(rec)) ).groupByKey().flatmap( lambda redc: ordered (rec[1],key=rec[1].split(",")[1],FALSE)) 

rdd1 = sc.textFile("/user/cloudera/orderitems/*")

rdd1 = rdd1.filter( lambda rec : int(rec.split(",")[1]) == 2)

rdd1 = rdd1.map( lambda rec : (rec.split(",")[1],str(rec)) )

rdd1 = rdd1.groupByKey()

rdd2  = rdd1.flatMap( lambda rec: sorted(rec[1],key= ( lambda rec: float(rec.split(",")[4]) ),reverse=False))


read as various data types (avro, parquet, json, hive metastore)

write as various data types

sqoop import with columns with query

sqoop export with columns

filter

count/reduce by (city|state|count) from master table of employee address(single table)

when writing write without commas (delimited by tabs)


    val data = Array(1, 2, 3, 4, 5)                     // create Array of Integers
    val dataRDD = sc.parallelize(data)                  // create an RDD
    val dataDF = dataRDD.toDF()                         // convert RDD to DataFrame
    dataDF.write.parquet("data.parquet")                // write to parquet
    val newDataDF = sqlContext.
                    read.parquet("data.parquet")        // read back parquet to DF
    newDataDF.show()                                    // show contents

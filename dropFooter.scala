val df = spark.read.textFile("/user/bharath/sample.txt")
df.show
val rdd = df.rdd
val count = rdd.count.toInt
val finalRDD = rdd.take(count).dropRight(1)
val finalDF = spark.sparkContext.parallelize(finalRDD).toDF
finalDF.show

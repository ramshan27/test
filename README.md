# test
test
rdd1 = rdd1.filter( lambda rec : int(rec.split(",")[1]) == 2).map( lambda rec : (rec.split(",")[1],str(rec)) ).groupByKey().flatmap( lambda redc: ordered (rec[1],key=rec[1].split(",")[1],FALSE)) 

rdd1 = sc.textFile("/user/cloudera/orderitems/*")

rdd1 = rdd1.filter( lambda rec : int(rec.split(",")[1]) == 2)

rdd1 = rdd1.map( lambda rec : (rec.split(",")[1],str(rec)) )

rdd1 = rdd1.groupByKey()

rdd2  = rdd1.flatMap( lambda rec: sorted(rec[1],key= ( lambda rec: float(rec.split(",")[4]) ),reverse=False))

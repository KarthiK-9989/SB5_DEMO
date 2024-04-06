import org.apache.spark.SparkContext

object second {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "first-program")

//    val mylist=List("karthik is good boy",
//    "karthik goes to office daily",
//      "karthik is good boy",
//   "karthik goes to office daily",
//     "karthik is good boy",
//        "karthik goes to office daily"
//    )
//
//    val rdd1=sc.parallelize(mylist)


    val rdd1 = sc.textFile("C:/Users/Karthik Kondpak/Desktop/data3.txt")
    val rdd2 = rdd1.flatMap(x=>x.split(" "))
    val rdd3 = rdd2.map(x=>(x,1))
    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
     rdd4.collect.foreach(println)




//
//    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5,6))
//    val sum = rdd.reduce(_+_)
//    val count=rdd.count()
//    val avg=sum/count.toDouble
//    println(avg)

//
//    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val filteredRdd = rdd.filter(x => x % 2 != 0)
//    val count = filteredRdd.count()
//    println("Count of odd numbers: " + count)

//
//    val rdd1 = sc.parallelize(Array((1, "apple"), (2, "banana"), (3,
//      "orange")))
//    val rdd2 = sc.parallelize(Array((1, "red"), (2, "yellow"), (4,
//      "green")))
//
//    val rdd3=rdd1.join(rdd2)
//
//     rdd3.collect.foreach(println)

//    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val rdd2 = sc.parallelize(Array(3, 4, 5,10))
//
//    val rdd3=rdd2.subtract(rdd1)
//
//    rdd3.collect.foreach(println)

//    val rdd = sc.parallelize(Array("apple", "banana", "orange",
//      "pear", "grape"))
//    val searchTerm = "oran"
//    val filteredRdd = rdd.filter(x => x == searchTerm)
//
//    filteredRdd.collect.foreach(println)

//    val rdd = sc.parallelize(Array("apple", "banana", "orange",
//      "pear", "grape"))
//    val searchTerm = "an"
//    val filteredRdd = rdd.filter(x => x.contains(searchTerm))
//    filteredRdd.collect.foreach(println)


scala.io.StdIn.readLine()

  }
}
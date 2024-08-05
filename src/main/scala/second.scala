import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object second {

  def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder
          .appName("SCD Type 1 Example")
          .master("local[*]")
          .getOrCreate()

        import spark.implicits._

        // Initial DataFrame
        val initialData = List(
          (1, "John Doe", "New York", "2023-01-01"),
          (2, "Jane Roe", "Chicago", "2023-01-01"),
          (3, "Sam Spade", "Los Angeles", "2023-01-01")
        ).toDF("ID", "Name", "City", "Last_Upd")

        // New DataFrame
        val newData = Seq(
          (1, "John Doe", "Boston", "2023-07-01"),
          (2, "Jane Roe", "Chicago", "2023-01-01"),
          (4, "Max Payne", "Miami", "2023-07-01")
        ).toDF("ID", "Name", "City", "Last_Upd")

        // Show initial data
        println("Initial Data:")
        initialData.show()

        // Show new data
        println("New Data:")
        newData.show()

        // Perform SCD Type 1 update
        val updatedData = initialData.as("initial")
          .join(newData.as("new"), col("ID"), "outer")
          .select(
            coalesce($"new.ID", $"initial.ID").as("ID"),
            coalesce($"new.Name", $"initial.Name").as("Name"),
            coalesce($"new.City", $"initial.City").as("City"),
            coalesce($"new.Last_Upd", $"initial.Last_Upd").as("Last_Upd")
          )

        // Show updated data
        println("Updated Data:")
        updatedData.show()

        spark.stop()




//
//    val rdd1 = sc.textFile("C:/Users/Karthik Kondpak/Desktop/2nd meetup pics/biglog.txt")
//    val wordCounts = rdd1
//      .flatMap(line => line.split(":")(0).toLowerCase.split(" ")) // Split by colon, lowercase, and split by spaces
//      .map(word => (word, 1))
//      .reduceByKey((x, y) => x + y)
//
//    wordCounts.collect.foreach { case (word, count) => println(s"Word: $word, Count: $count") }

//    I'd be glad to help you with that! Here's the improved RDD code that addresses the issues you mentioned and provides word counts for "WARN", "ERROR", and other words:
//
//      Scala
//    val rdd1 = sc.textFile("C:/Users/Karthik Kondpak/Desktop/2nd meetup pics/biglog.txt")
//
//    // Filter lines starting with "WARN:" (case-insensitive)
//    val filteredRDD = rdd1.filter(line => line.startsWith("WARN:", true))
//
//    // Split each line by colon (:) and extract the first word (assuming the warning/error level is the first word)
//    val wordCounts = filteredRDD
//      .flatMap(line => line.split(":")(0).toLowerCase.split(" ")) // Split by colon, lowercase, and split by spaces
//      .map(word => (word, 1))
//      .reduceByKey((x, y) => x + y)
//
//    // Collect the results and print them
//    wordCounts.collect.foreach { case (word, count) => println(s"Word: $word, Count: $count") }




    //    val mylist=List("karthik is good boy",
    //    "karthik goes to office daily",
    //      "karthik is good boy",
    //   "karthik goes to office daily",
    //     "karthik is good boy",
    //        "karthik goes to office daily"
    //    )
    //
    //    val rdd1=sc.parallelize(mylist)


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
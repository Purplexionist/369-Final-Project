import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object driver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    val conf = new SparkConf().setAppName("OhhYeah").setMaster("local[10]")
    val sc = new SparkContext(conf)

    //get groupByPitchers
    val training = sc.textFile("train.txt")
    val batters = training.map(x => x.split(',')).map(x => (x(4),(x(8).toDouble.toInt,x(9).toDouble.toInt,x(10).toDouble.toInt,1))).persist()
    val groupedBatters = batters.reduceByKey((x,y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4)).mapValues(x => (x._1*1.0/x._4,x._2*1.0/x._4,x._3*1.0/x._4)).persist()
    val batMap = groupedBatters.collectAsMap()
    val pitchers = training.map(x => x.split(',')).map(x => (x(5),(x(8).toDouble.toInt,x(9).toDouble.toInt,x(10).toDouble.toInt,1))).persist()
    val groupedPitchers = pitchers.reduceByKey((x,y) => (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4)).mapValues(x => (x._1*1.0/x._4,x._2*1.0/x._4,x._3*1.0/x._4)).persist()
    val pitchMap = groupedPitchers.collectAsMap()
    val league = training.map(x => (x(8).toDouble.toInt,x(9).toDouble.toInt,x(10).toDouble.toInt)).reduce((x,y) => (x._1+y._1,x._2+y._2,x._3+y._3))
    val finalLeague = (league._1/200000.0, league._2/200000.0,league._3/200000.0)
    println(finalLeague)
    //groupedPitchers.take(100).foreach(println)


    //do computation
    //val league = 2
    //val stuff = sc.textFile("things.txt")
    //val ans = stuff.map(x => ("bat", "pitch"),(groupByBatter("bat")*groupByPitcher("pitch")/league)/((groupByBatter("bat")*groupByPitcher("pitch")/league)+((1-groupByBatter("bat"))*(1-groupByPitcher("pitch"))/(1-league))))
  }

  def groupByBatter(batter: String): Double = {
    return  .5
  }

  def groupByPitcher(pitcher: String): Double = {
    return 2.0
  }
}

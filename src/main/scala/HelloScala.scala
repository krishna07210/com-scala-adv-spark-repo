import com.config.CommonUtils

object HelloScala {

  def main(args: Array[String]): Unit = {
    println("Hello Scala..!")
    val resourcesPath = getClass.getResource("/srcData")
    println(resourcesPath.getPath)
   val path = CommonUtils.getInputFilePath(args,"/flight-data/csv/2015-summary.csv")
    println("path -> "+path)

    var path1 :String = null
    if (path1 ==null){
      println("path is null")
    }
  }
}

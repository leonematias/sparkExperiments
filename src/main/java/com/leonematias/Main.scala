package com.leonematias

import com.leonematias.common.Utils

object Main {

  def main(args: Array[String]): Unit = {

    val spark = Utils.initSpark()
    //import spark.implicits._

    val numDS = spark.range(5, 100, 5)

    numDS.show(10)

    numDS.describe().show()


  }

}

package com.scala.learn

import org.apache.spark.sql.SparkSession
import scala.math.random

class Test {
    def m(x: Int): Int = {
        return x + 3
    }
    val f = (x: Int) => x + 3

    def printMe(): Unit = { //Unit is equal to void
        println("Hello, Scala!")
    }

    def sum_demo(x: Int): Int = {
        var a = 0
        var res = 0
        val numList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        for (a <- 1 to x
             if a % 2 == 0; if a % 7 == 0) {
            res += a
        }
        a = 0
        for (a <- numList
             if a % 2 == 0
        ) {
            res += a
        }
        var retVal = for (x <- numList
                          if x % 2 == 0) yield x
        for (y <- retVal) {
            println("Value of y: " + y);
            res += y
        }
        return res
    }
}

/** Computes an approximation to pi */
object SparkDemo {
    def calculate_pi(slices: Int): Double = {
        val spark = SparkSession
            .builder
            .appName("SparkLearn")
            .getOrCreate()
        val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
        val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
            val x = random * 2 - 1
            val y = random * 2 - 1
            if (x * x + y * y <= 1) 1 else 0
        }.reduce(_ + _)
        var res = 4.0 * count / (n - 1)
        spark.stop()
        return res;
    }

    def main(args: Array[String]) {
        val slices = if (args.length > 0) args(0).toInt else 2
        calculate_pi(slices)
        var t = new Test
        t.printMe()
        var r = t.sum_demo(1000)
        println(s"result = " + r)
        var r2 = calculate_pi(10);
    }
}

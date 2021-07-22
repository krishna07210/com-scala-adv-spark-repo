package com.spark.components.SharedVariables

import org.apache.spark.util.AccumulatorV2

class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
  private var num: BigInt = 0

  def reset(): Unit = {
    this.num = 0
  }

  def add(intValue: BigInt): Unit = {
    if (intValue % 2 == 0) {
      this.num += intValue
    }
  }

  def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = {
    this.num += other.value
  }

  def value(): BigInt = {
    this.num
  }

  def copy(): AccumulatorV2[BigInt, BigInt] = {
    new EvenAccumulator
  }

  def isZero(): Boolean = {
    this.num == 0
  }
}


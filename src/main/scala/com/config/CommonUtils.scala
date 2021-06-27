package com.config

import java.io.File

import scala.reflect.io.Directory

/**
 * Created by krish on 12-04-2020.
 * Object to return the File path
 */
object CommonUtils {
  // a regular expression which matches commas but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  def getInputFilePath(fileName: String): String = {
    val filePath: String = Constants.inputSource + fileName
    return filePath
  }

  def getOutputFilePath(fileName: String): String = {
    val filePath: String = Constants.outputSource + fileName
    val directory = new Directory(new File(filePath))
    directory.deleteRecursively()
    return filePath
  }
}

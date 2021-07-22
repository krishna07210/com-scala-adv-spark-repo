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
  var InputPath: String = null
  var OutputPath: String = null
  var isUserInputFile: String = "N"

  def getInputFilePath(args: Array[String], fileName: String): String = {
    checkUserInput(args)
    if (isUserInputFile.equals("Y")) {
      return InputPath
    } else {
      return InputPath + fileName
    }
  }

  def getOutputFilePath(args: Array[String], fileName: String): String = {
    checkUserInput(args)
    return OutputPath + fileName
  }

  def checkUserInput(args: Array[String]): Unit = {
    if (args.length > 0) {
      if (args(0).toUpperCase.equals("F")) {
        isUserInputFile = "Y"
      }
      if (args.length == 1) {
        InputPath = args(0)
      } else if (args.length > 1) {
        InputPath = args(0)
        OutputPath = args(1)
      } else {
        InputPath = getInputResourcesPath()
        OutputPath = getOutputResourcesPath
      }
    }
  }

  def getInputResourcesPath(): String = {
    return getClass.getResource("/srcData/").toString
  }

  def getOutputResourcesPath(): String = {
    return getClass.getResource("/trgData/").toString
  }

  //  def getInputFilePath(args,fileName: String): String = {
  //    val filePath: String = getInputResourcesPath() + fileName
  //    return filePath
  //  }
  //
  //  def getOutputFilePath(args,fileName: String): String = {
  //    val filePath: String = getOutputResourcesPath() + fileName
  //    val directory = new Directory(new File(filePath))
  //    directory.deleteRecursively()
  //    return filePath
  //  }

}

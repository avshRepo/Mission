package models.operations

import models.OperationsResultFile

/**
  * This operation represents all the mathematical operations
  *
  * @param operationType - the type of mathmatical operation we want to execute
  */
case class MathOperation(operationType: MathOperationType.Value) extends Operation {
  override def getResult(file: OperationsResultFile)(
      implicit workingDirectory: String): OperationsResultFile = {
    // create new file
    val outputFile = OperationsResultFile.createNewFile()

    // get previous file metadata - meaning name and is numerical
    val columnMetadata =
      file.getColumnMetadata(OperationsResultFile.defaultColumn)

    // check if there is numerical value else we return empty file
    if (columnMetadata.isNumerical) {
      // get column stream
      val source = file.getColumnAsStream(OperationsResultFile.defaultColumn)

      // turn the column stream to be double stream - this is done because we know we have numerical values
      val linesAsDoubleStream =
        source.getLines().toStream.tail.tail.map(_.toDouble)

      // add metadata to output file
      val fileStream = outputFile.writeNewColumn(
        OperationsResultFile.defaultColumn,
        columnMetadata.name,
        columnMetadata.isNumerical)

      // calc the values we want to write according to the chosen operation
      val calculatedValues: Seq[Double] = operationType match {
        case MathOperationType.MAX => Seq(linesAsDoubleStream.max)
        case MathOperationType.MIN => Seq(linesAsDoubleStream.min)
        case MathOperationType.SUM => Seq(linesAsDoubleStream.sum)
        case MathOperationType.AVG =>
          Seq(linesAsDoubleStream.sum / linesAsDoubleStream.size)
        case MathOperationType.CEIL =>
          linesAsDoubleStream.map(value => Math.ceil(value))
      }

      // write the calculated value to the file
      calculatedValues.foreach { value =>
        {
          fileStream.writeLine(value.toString)
        }
      }

      source.close()
      fileStream.close()
    } else {
      // add metadata to output file
      val fileStream = outputFile.writeNewColumn(
        OperationsResultFile.defaultColumn,
        columnMetadata.name,
        columnMetadata.isNumerical)

      fileStream.close()

    }

    outputFile
  }
}

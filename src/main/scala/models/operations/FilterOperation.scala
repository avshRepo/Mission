package models.operations

import models.OperationsResultFile

/**
 * This operation allows us to filter rows that has the given value at the given column number
 *
 * @param columnNumber - the number of the column that we to check
 * @param valueToMatch - the value we want to check
 */
case class FilterOperation(columnNumber: Int, valueToMatch: String) extends Operation {
  override def getResult(file: OperationsResultFile)(implicit workingDirectory: String): OperationsResultFile = {
    // create new output file
    val outputFile = OperationsResultFile.createNewFile()

    // get previous file metadata - meaning name and is numerical
    val columnMetadata = file.getColumnMetadata(columnNumber)

    // get the indexes of the rows that has the valueToMatch at the given column number
    val indexesThatMatch = file.getIndexsThatMatch(columnNumber, valueToMatch, columnMetadata.isNumerical)

    // get the number of columns
    val numberOfColumns = file.getNumberOfColumns()

    // copy just the rows that their indexes match
    // we do it column by column because we dont save lines
    (0 until numberOfColumns).foreach {
      columnNumber => {
        file.copyColumn(columnNumber, outputFile, true, indexesThatMatch)
      }
    }

    outputFile
  }
}

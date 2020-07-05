package models

/**
 * This class contains all the need metadata in order to process the files
 *
 * @param name - the column name
 * @param isNumerical - is the column has numerical values only
 */
case class ColumnMetadata(name: String, isNumerical: Boolean)

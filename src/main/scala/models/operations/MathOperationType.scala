package models.operations

/**
 * This object contains enum the defines all the math operations types
 */
object MathOperationType extends Enumeration {
  type OperationType = Value

  val MAX, MIN, SUM, AVG, CEIL = Value
}

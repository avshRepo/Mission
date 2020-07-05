import models.operations.{FilterOperation, MathOperation, Operation, MathOperationType, PluckOperation}

object Main {
  def main(args: Array[String]): Unit = {

    val csvPath = "inputData/0.csv"

    val workingDirectoryPath = "workingDir/"
    val operations: Seq[Operation] = Seq(FilterOperation(3, "Iowa"), PluckOperation(10), MathOperation(MathOperationType.MAX))

    val resultFilePath = ChainExecutor.executeChainOfOperations(csvPath, workingDirectoryPath, operations)

    println("the result file is at this directory " + resultFilePath)

  }
}

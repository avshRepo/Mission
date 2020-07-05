package models.operations

import models.OperationsResultFile

/**
 * This trait represents the default contract for building operation
 */
trait Operation {
  /**
   * This method is the one that allows us to do the chaining. it receives operations result file
   * and return operations result file
   *
   * @param file - the file that we work on
   * @param workingDirectory - our current working directory
   *
   * @return - the file that the next operation need to the work on
   */
  def getResult(file: OperationsResultFile)(implicit workingDirectory: String): OperationsResultFile
}

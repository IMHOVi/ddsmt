package ru.imho.ddsmt.params

/**
 * Created by skotlov on 11/13/14.
 */
trait Param {

  def applyToString(str: String, paramName: String): String
}

class ApplyParamException extends RuntimeException

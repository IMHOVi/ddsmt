package ru.imho.ddsmt

import java.io.Closeable

/**
 * Created by skotlov on 11/14/14.
 */
object Utils {

  def using[T <: Closeable, R]
      (resource: T)
      (block: T => R): R = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }
}

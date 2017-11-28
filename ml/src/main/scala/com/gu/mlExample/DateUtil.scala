package com.gu.mlExample

import java.time.temporal.ChronoUnit

object DateUtil {
  // excluding to
  def getDatesBetween(from: String, to: String): Seq[String] = {
    getDatesBetween(java.time.LocalDate.parse(from), java.time.LocalDate.parse(to)).map(_.toString)
  }

  // excluding to
  def getDatesBetween(from: java.time.LocalDate, to: java.time.LocalDate): Seq[java.time.LocalDate] = {
    Range(0, ChronoUnit.DAYS.between(from, to).toInt)
      .map(i => from.plusDays(i))
  }


}

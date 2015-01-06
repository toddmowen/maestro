//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.core.exec

import java.io.{File, InputStream}
import java.util.UUID

import scala.util.Failure

import scalaz.effect.IO

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import org.specs2.specification.BeforeExample

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import au.com.cba.omnia.thermometer.fact.Fact
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.tools.Streams
import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerRecordReader, ThermometerSpec}, Thermometer._

import au.com.cba.omnia.maestro.core.exec.ParlourInstances._
import au.com.cba.omnia.maestro.core.task.CustomerImport


object SqoopImportExecutionSpec
  extends ThermometerSpec
  with BeforeExample
  with SqoopExecution { def is = s2"""
  Sqoop Import Execution test
  ===========================

  imports data from DB to HDFS successfully    $endToEndImportWithSuccess
  imports data from DB to HDFS using SQL query $endToEndImportWithSqlQuery
  handles exceptions while importing data      $endToEndImportWithException
  handles exceptions while table not set       $endToEndImportWithoutTable
"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val hdfsRoot         = s"$dir/user/hdfs"
  val mapRedHome       = s"${System.getProperty("user.home")}/.ivy2/cache"
  val importTableName  = s"customer_import_${UUID.randomUUID.toString.replace('-', '_')}"
  val dirStructure     = s"sales/books/$importTableName"
  val hdfsLandingPath  = s"$hdfsRoot/source/$dirStructure"
  val hdfsArchivePath  = s"$hdfsRoot/archive/$dirStructure"
  val timePath         = List("2014", "10", "10") mkString File.separator

  val options = SqoopImportConfig.options[ParlourImportDsl](
    connectionString, username, password, importTableName
  ).splitBy("id")

  SqoopExecutionTest.setupEnv()

  val compressedRecordReader =
    ThermometerRecordReader[String]((conf, path) => IO {
      val ctx = new Context(conf)
      val in = new GzipCompressorInputStream(
        ctx.withFileSystem[InputStream](_.open(path))("compressedRecordReader")
      )
      try Streams.read(in).lines.toList
      finally in.close
    })

  def endToEndImportWithSuccess = {
    val config = SqoopImportConfig(hdfsLandingPath, hdfsArchivePath, timePath, options)
    val (path, count) = executesSuccessfully(sqoopImport(config))
    facts(
      ImportPathFact(path),
      s"$hdfsLandingPath/$timePath" </> "part-m-00000" ==> lines(CustomerImport.data),
      s"$hdfsArchivePath/$timePath" </> "part-00000.gz" ==> records(compressedRecordReader, CustomerImport.data)
    )
    count must_== 3
  }

  def endToEndImportWithSqlQuery = {
    val optionsWithQuery = SqoopImportConfig.optionsWithQuery[ParlourImportDsl](connectionString, username, password,
      s"SELECT * FROM $importTableName WHERE $$CONDITIONS", Some("id"))

    val config = SqoopImportConfig(hdfsLandingPath, hdfsArchivePath, timePath, optionsWithQuery)
    val (path, count) = executesSuccessfully(sqoopImport(config))
    facts(
      ImportPathFact(path),
      s"$dir/user/hdfs/archive/sales/books" </> importTableName </> "2014/10/10" </> "part-00000.gz" ==>
        records(compressedRecordReader, CustomerImport.data),
      s"$dir/user/hdfs/source/sales/books" </> importTableName </> "2014/10/10" </> "part-m-00000"   ==>
        lines(CustomerImport.data)
    )
    count must_== 3
  }

  def endToEndImportWithException = {
    val config = SqoopImportConfig(hdfsLandingPath, hdfsArchivePath, timePath, options)
    execute(sqoopImport(config, "very_bad_col=1")) must beLike { case Failure(_) => ok }
  }

  def endToEndImportWithoutTable = {
    val config = SqoopImportConfig(
      hdfsLandingPath, hdfsArchivePath, timePath, options.tableName(null)
    )
    execute(sqoopImport(config)) must beLike { case Failure(_) => ok }
  }

  override def before: Any = CustomerImport.tableSetup(
    connectionString, username, password, importTableName
  )

  object ImportPathFact {
    def apply(actualPath: String) = Fact(_ => actualPath must beEqualTo(s"$hdfsLandingPath/$timePath"))
  }
}

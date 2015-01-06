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

package au.com.cba.omnia.maestro.core.task

import java.io.File

import com.cloudera.sqoop.SqoopOptions

import org.apache.log4j.Logger

import org.apache.hadoop.io.compress.GzipCodec

import com.twitter.scalding.{Args, Job, Mode, TextLine, TypedPipe}

import cascading.flow.FlowDef

import com.cba.omnia.edge.source.compressible.CompressibleTypedTsv

import au.com.cba.omnia.parlour.SqoopSyntax.{ParlourExportDsl, ParlourImportDsl}
import au.com.cba.omnia.parlour._


/**
 * Import and export data between a database and HDFS.
 *
 * All methods return Jobs that can be added to a cascade.
 *
 * See the example at `au.com.cba.omnia.maestro.example.CustomerSqoopExample` to understand how to use
 * the [[Sqoop]] API
 */
trait Sqoop {

  /**
   * Convenience method to populate a parlour import option instance
   *
   * @param connectionString: database connection string
   * @param username: database username
   * @param password: database password
   * @param dbTableName: Table name in database
   * @param outputFieldsTerminatedBy: output field terminating character
   * @param nullString: The string to be written for a null value in columns
   * @param whereCondition: where condition if any
   * @param options: parlour option to populate
   * @return : Populated parlour option
   */
  def createSqoopImportOptions[T <: ParlourImportOptions[T]](
    connectionString: String,
    username: String,
    password: String,
    dbTableName: String,
    outputFieldsTerminatedBy: Char,
    nullString: String,
    whereCondition: Option[String] = None,
    options: T = ParlourImportDsl()
  ): T = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withEntity = withConnection
      .tableName(dbTableName)
      .fieldsTerminatedBy(outputFieldsTerminatedBy)
      .nullString(nullString)
      .nullNonString(nullString)
    whereCondition.map(withEntity.where).getOrElse(withEntity)
  }

  /**
   * Convenience method to populate a parlour import option instance.
   * Use it when you want to use SQL Select query to fetch data.
   *
   * @param connectionString: database connection string
   * @param username: database username
   * @param password: database password
   * @param query: SQL Select query
   * @param outputFieldsTerminatedBy: output field terminating character
   * @param nullString: The string to be written for a null value in columns
   * @param splitBy: splitting column; if None, then `numberOfMappers` is set to 1
   * @param options: parlour option to populate
   * @return : Populated parlour option
   */
  def createSqoopImportOptionsWithQuery[T <: ParlourImportOptions[T]](
    connectionString: String,
    username: String,
    password: String,
    query: String,
    splitBy: Option[String],
    outputFieldsTerminatedBy: Char,
    nullString: String,
    options: T = ParlourImportDsl()
  ): T = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withSplitBy = splitBy.fold(withConnection.numberOfMappers(1))(withConnection.splitBy(_))
    withSplitBy
      .sqlQuery(query)
      .fieldsTerminatedBy(outputFieldsTerminatedBy)
      .nullString(nullString)
      .nullNonString(nullString)
  }

  /**
   * Run a custom sqoop import using parlour import options
   *
   * '''Use this method ONLY if non-standard settings are required'''
   *
   * @param options: Parlour import options
   */
  def customSqoopImport(
    options: ParlourImportOptions[_]
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val logger = Logger.getLogger("Sqoop")
    val sqoopOptions = options.toSqoopOptions
    logger.info("Start of sqoop import")
    logger.info(s"connectionString = ${sqoopOptions.getConnectString}")
    logger.info(s"tableName        = ${sqoopOptions.getTableName}")
    logger.info(s"targetDir        = ${sqoopOptions.getTargetDir}")
    new ImportSqoopJob(options)(args)
  }

  /**
   * Runs a sqoop export from HDFS to a database table.
   *
   * If 'deleteFromTable' param is true, then all preexisting rows from the target DB table will be deleted first.
   * Otherwise the rows will be appended.
   *
   * @param options: Custom export options
   * @param deleteFromTable: Delete all the rows before export
   * @return Job for this export
   */
  def customSqoopExport[T <: ParlourExportOptions[T]](
    options: ParlourExportOptions[T], deleteFromTable: Boolean = false
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val withDelete = if (deleteFromTable) SqoopDelete.trySetDeleteQuery(options) else options
    new ExportSqoopJob(withDelete)(args)
  }

  /**
   * Runs a sqoop export from HDFS to a database table.
   *
   * If 'deleteFromTable' param is true, then all preexisting rows from the target DB table will be deleted first.
   * Otherwise the rows will be appended.
   *
   * @param exportDir: Directory containing data to be exported
   * @param tableName: Table name in the database
   * @param connectionString: Jdbc url for connecting to the database
   * @param username: Username for connecting to the database
   * @param password: Password for connecting to the database
   * @param inputFieldsTerminatedBy: Field separator in input data
   * @param inputNullString: The string to be interpreted as null for string and non string columns
   * @param options: Extra export options
   * @param deleteFromTable: Delete all the rows before export
   * @return Job for this export
   */
  def sqoopExport[T <: ParlourExportOptions[T]](
    exportDir: String,
    tableName: String,
    connectionString: String,
    username: String,
    password: String,
    inputFieldsTerminatedBy: Char,
    inputNullString: String,
    options: T = ParlourExportDsl(),
    deleteFromTable: Boolean = false
  )(args: Args)(implicit flowDef: FlowDef, mode: Mode): Job = {
    val withConnection = options.connectionString(connectionString).username(username).password(password)
    val withEntity = withConnection.exportDir(exportDir)
      .tableName(tableName)
      .inputFieldsTerminatedBy(inputFieldsTerminatedBy)
      .inputNull(inputNullString)
    customSqoopExport(withEntity, deleteFromTable)(args)
  }
}

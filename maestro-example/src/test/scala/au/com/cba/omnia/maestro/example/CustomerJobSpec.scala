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

package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.maestro.example.thrift.Customer

object CustomerJobSpec
  extends ThermometerSpec
  with Records
  with HiveSupport { def is = s2"""

Customer Job
============

  end to end pipeline        $pipeline

"""

  def pipeline = {
    val actualReader   = ParquetThermometerRecordReader[Customer]
    val expectedReader = delimitedThermometerRecordReader[Customer]('|', "null", implicitly[Decode[Customer]])

    withEnvironment(path(getClass.getResource("/customer").toString)) {
      val args = Map(
        "hdfs-root"    -> List(s"$dir/user"),
        "local-root"   -> List(s"$dir/user"),
        "archive-root" -> List(s"$dir/user/archive")
      )
      executesSuccessfully(CustomerJob.job, args) must_== JobFinished

      facts(
        hiveWarehouse </> "customer_customer.db" </> "by_date" ==> recordsByDirectory(actualReader, expectedReader, "expected" </> "customer" </> "by-date")
      )
    }
  }
}

package au.com.cba.omnia.maestro.core.hdfs

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

class GuardSpec extends ThermometerSpec { def is = s2"""

HDFS Guard properties
=====================

  glob expands to matching dirs                   $globIncludesDirectories
  expandPaths skips processed dirs                $skipsProcessed
  expandTransferredPaths skips uningested dirs    $skipsUningested
"""

  def globIncludesDirectories = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/a*") must_== List(
        s"file:$dir/user/a",
        s"file:$dir/user/a1"
        // excludes "a2" because it is a file
        // excludes "b" because it doesn't match the glob
      )
    }
  }

  def skipsProcessed = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/c*") must_== List(
        s"file:$dir/user/c",
        s"file:$dir/user/c_transferred"
        // excludes "c_processed"
      )
    }
  }

  def skipsUningested = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandTransferredPaths(s"$dir/user/c*") must_== List(
        s"file:$dir/user/c_transferred"
        // excludes "c"
        // excludes "c_processed"
      )
    }
  }

}

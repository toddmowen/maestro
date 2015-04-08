package au.com.cba.omnia.maestro.core.hdfs

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

class GuardSpec extends ThermometerSpec { def is = s2"""

HDFS Guard properties
=====================

  expandPaths matches globbed dirs                $matchesGlobbedDirs
  expandPaths skips files                         $skipsFiles
  expandPaths skips processed dirs                $skipsProcessed
  expandTransferredPaths skips uningested dirs    $skipsUningested
"""

  def matchesGlobbedDirs = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/a*") must_== List(
        s"file:$dir/user/a",
        s"file:$dir/user/a1"
        // excludes various other directories that don't match the glob
      )
    }
  }

  def skipsFiles = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/b*") must_== List(
        s"file:$dir/user/b1"
        // excludes "b2" because it's not a directory
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

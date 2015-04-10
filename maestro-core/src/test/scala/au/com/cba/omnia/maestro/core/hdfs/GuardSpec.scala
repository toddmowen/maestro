package au.com.cba.omnia.maestro.core.hdfs

import java.io.File

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

class GuardSpec extends ThermometerSpec { def is = s2"""

HDFS Guard properties
=====================

  expandPaths:
    matches globbed dirs               $expandPaths_matchesGlobbedDirs
    skips files                        $expandPaths_skipsFiles
    skips processed dirs               $expandPaths_skipsProcessed

  expandTransferredPaths:
    skips uningested dirs              $expandTransferredPaths_skipsUningested

  listNonEmptyFiles:
    lists non-empty files              $listNonEmptyFiles_listsNonEmptyFiles
    skips subdirectories               $listNonEmptyFiles_skipsSubdirectories

  createFlagFile:
    creates _PROCESSED                 $createFlagFile_createsPROCESSED
"""

  def expandPaths_matchesGlobbedDirs = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/a*") must_== List(
        s"file:$dir/user/a",
        s"file:$dir/user/a1"
        // excludes various other directories that don't match the glob
      )
    }
  }

  def expandPaths_skipsFiles = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/b*") must_== List(
        s"file:$dir/user/b1"
        // excludes "b2" because it's not a directory
      )
    }
  }

  def expandPaths_skipsProcessed = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths(s"$dir/user/c*") must_== List(
        s"file:$dir/user/c",
        s"file:$dir/user/c_transferred"
        // excludes "c_processed"
      )
    }
  }

  def expandTransferredPaths_skipsUningested = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandTransferredPaths(s"$dir/user/c*") must_== List(
        s"file:$dir/user/c_transferred"
        // excludes "c"
        // excludes "c_processed"
      )
    }
  }

  def listNonEmptyFiles_listsNonEmptyFiles = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.listNonEmptyFiles(List(s"$dir/user/a", s"$dir/user/c")) must_== List(
        s"file:$dir/user/c/c.dat"
        // excludes "a/a.dat" because it is zero-length
      )
    }
  }

  def listNonEmptyFiles_skipsSubdirectories = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.listNonEmptyFiles(List(s"$dir/user")) must_== List(
        s"file:$dir/user/b2"
        // excludes all subdirectories
      )
    }
  }

  def createFlagFile_createsPROCESSED = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.createFlagFile(List(s"$dir/user/a1", s"$dir/user/b1"))
      new File(s"$dir/user/a1/_PROCESSED").exists() must beTrue
      new File(s"$dir/user/b1/_PROCESSED").exists() must beTrue
    }
  }

}

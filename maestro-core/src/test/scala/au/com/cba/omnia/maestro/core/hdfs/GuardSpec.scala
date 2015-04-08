package au.com.cba.omnia.maestro.core.hdfs

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

class GuardSpec extends ThermometerSpec { def is = s2"""

HDFS Guard properties
=====================

  glob expands to matching directories            $globIncludesDirectories
"""

  def globIncludesDirectories = {
    withEnvironment(path(getClass.getResource("/hdfs-guard").toString)) {
      Guard.expandPaths("a*") must_== List(
        s"file:$dir/user/a",
        s"file:$dir/user/a1"
        // excludes "a2" because it is a file
        // excludes "b" because it doesn't match the glob
      )
    }
  }

}

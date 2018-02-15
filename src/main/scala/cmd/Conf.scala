package cmd

import inputdata.MovieLensDataHolder

class Conf(arguments: Seq[String])  {

  val data = new MovieLensDataHolder("./input")

}

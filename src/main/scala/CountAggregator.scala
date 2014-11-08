import akka.actor.{ActorLogging, Actor}

class CountAggregator(threadCount: Int) extends Actor {
  val wordCountMap = scala.collection.mutable.Map[String, Int]()
  var count: Integer = 0;
  def receive = {
      case localCount: scala.collection.mutable.Map[String, Int] =>
        //        count = count + 1
        localCount map (x => wordCountMap += ((x._1, wordCountMap.getOrElse(x._1, 0) + x._2)))
        count = count + 1
        if (count == threadCount) {
          println("Got the completion message ... khallas!")
          onCompletion
        }
    }

  // print final word count on output
  def onCompletion() {

        for (word <- wordCountMap.keys) {
          print(word + "=>")
          println(wordCountMap.get(word).get)
        }
        print(s"Completed at ==>")
        println(System.currentTimeMillis())

  }

}

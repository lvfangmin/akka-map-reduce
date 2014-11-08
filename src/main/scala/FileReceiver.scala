import akka.actor.{ActorLogging, Actor}

class FileReceiver extends Actor {

  def receive = {
      case fileName: String =>
        val system = ActorSystem("receiversystem")
        val global = system.actorOf(Props(new CountAggregator(8)))
        val lineCollector = system.actorOf(Props(new LineCollector(system.actorOf(Props(new LocalAggregator(global))))))
        val router = system.actorOf(Props(new LineCollector(system.actorOf(Props(new LocalAggregator(global))))).withRouter(RoundRobinRouter(nrOfInstances = 8)))
        print(s"Started at ==>")
        println(System.currentTimeMillis())

        // determine line boundaries for number of chunks
        val file = new File(fileName)
        val chunkSize = 300000
        val count  = file.length()/chunkSize

        for (i <- 0 to count.intValue()) {
          val start = i * chunkSize //0, 10,20
          val end = chunkSize + start // 10,20,30
          router ! (fileName, start, end)  //send chunks
        }

        val remaining = chunkSize*count
        router ! (fileName, remaining, file.length()) //send out remaining chunk

        router ! Broadcast (true) //broadcast end of day message!
    }
}

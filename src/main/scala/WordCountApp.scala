object WordCountRunner extends App {
  val receiverSystem = ActorSystem("receiversystem", ConfigFactory.load("application_remote"))
  val c = receiverSystem.actorOf(Props[FileReceiver],"receiver")
  c ! "~/w2"  // 1 GB file
}

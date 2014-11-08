import akka.actor.{ActorLogging, Actor}

class LineCollector(localAgg: ActorRef) extends Actor {
  def receive = {
      case (fileName: String, chunkStart: Int, chunkSize: Int) =>

        val file = new File(fileName)
        val channel = new RandomAccessFile(file, "r").getChannel();
        val mappedBuff = channel.map(FileChannel.MapMode.READ_ONLY, 0,file.length()) //map complete file

        // load only if it is not loaded!

        var endP = chunkSize
        // file size is greater than chunk
        if (endP >= file.length()) {
          endP = file.length().intValue - 1
        }

        if (chunkStart < file.length()) {
          var start = mappedBuff.get(chunkStart) // start character
          val startPosition = trail(chunkStart, mappedBuff, start, endP)

          var end = mappedBuff.get(endP) // end character

          val endPosition = if ((endP != file.length() - 1)) trail(endP, mappedBuff, end, endP) else endP // valid end character
          val stringBuilder = new StringBuilder(endPosition - startPosition)
          val size = endPosition - startPosition
          val byteArray = new Array[Byte](size)

          // prepare and send buffer to local combiner
          if (endPosition > startPosition) {
            for (i <- startPosition to endPosition) {
              val character = mappedBuff.get(i).asInstanceOf[Char]
              if (character == '\n') {
                stringBuilder.append(' ')
              } else {
                stringBuilder.append(character)
              }
            }
            localAgg ! stringBuilder.toString.split(" ").groupBy(x => x)  //sending chunks
          }

        }

      case (done: Boolean) =>
        localAgg ! done // end of day message
    }

  private def trail(startP: Int, charBuff: java.nio.MappedByteBuffer, start: Byte, length: Int): Int = {

    var s = start.asInstanceOf[Char]
    val position = startP
    var next = position

    // if start character is not space, keep backtracking to start with new character word
    if (position <= length) {
      while (!s.equals(' ') && position > 0) {
        s = charBuff.get(next).asInstanceOf[Char]
        next = next - 1
      }
    }

    if (position != next) next + 1 else position
  }

}

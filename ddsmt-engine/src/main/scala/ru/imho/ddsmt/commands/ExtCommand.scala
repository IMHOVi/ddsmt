package ru.imho.ddsmt.commands

import ru.imho.ddsmt.Base._

/**
 * Created by skotlov on 11/13/14.
 */
class ExtCommand(commands: Iterable[String]) extends Command {

  override def execute(input: Iterable[DataSet], output: Iterable[DataSet]): Unit = {
    import scala.collection.JavaConversions._
    val pb = new ProcessBuilder(commands.toList)
    val p = pb.start()
//    BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream())); todo
//    String s = "";
//    while((s = in.readLine()) != null){
//        System.out.println(s);
//    }
    val r = p.waitFor()
    if (r != 0) {
      throw new RuntimeException("cmd '%s' was executed with error. code - %d".format(commands, r))
    }
  }
}

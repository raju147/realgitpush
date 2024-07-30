package newpackage
object GeeksforGeeks{
  def main(args:Array[String]): Unit = {
    println(test(1));
  }

  def test(x:Int):String=x match{
    case 0 =>"Hello,Geeks!!"
    case 1 =>"Are you learning scala?"
    case _ =>"Good Luck!!"
  }
}
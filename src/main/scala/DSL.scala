import scala.collection.mutable

/**
  * Created by mistasse on 6/04/16.
  */

object max {
  def apply(attribute: Symbol)(implicit renv: Ref[Environment]): RelValue[_] = apply(renv.get.rel, attribute)
  def apply(rel: Relation, attribute: Symbol): RelValue[_] = {
    var max: Option[RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(attribute))
      if(max.isEmpty)
        max = Some(current)
      else if(current.:>(max.get) == BooleanValue.TRUE)
        max = Some(current)
    }
    return max.get
  }
}

object min {
  def apply(attribute: Symbol)(implicit renv: Ref[Environment]): RelValue[_] = apply(renv.get.rel, attribute)
  def apply(rel: Relation, attribute: Symbol): RelValue[_] = {
    var min: Option[RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(attribute))
      if(min.isEmpty)
        min = Some(current)
      else if(current.:<(min.get) == BooleanValue.TRUE)
        min = Some(current)
    }
    return min.get
  }
}

trait RelEnv {
  implicit val renv: Ref[Environment] = new Ref[Environment](new Environment())

  implicit def Sym2RelValue(a: Symbol)(implicit renv: Ref[Environment]): RelValue[_] = renv.get.record.get(renv.get.rel.offsets(a))
  implicit def Sym2Identifier(a: Symbol): Identifier = new Identifier(a)

  implicit def Rel2RelValue(a: Relation): RelValue[_] = new RelationValue(a)
  implicit def SymRel2RelValue(a: (Symbol, Relation)): (Symbol, RelValue[_]) = (a._1, new RelationValue(a._2))

  implicit def String2RelValue(a: String): RelValue[_] = StringValue(a)
  implicit def SymString2RelValue(a: (Symbol, String)): (Symbol, RelValue[_]) = (a._1, StringValue(a._2))

  implicit def Int2RelValue(a: Int): RelValue[_] = IntValue(a)
  implicit def SymInt2RelValue(a: (Symbol, Int)): (Symbol, RelValue[_]) = (a._1, IntValue(a._2))

  implicit def Bool2RelValue(a: Boolean): RelValue[_] = BooleanValue(a)
  implicit def SymBool2RelValue(a: (Symbol, Boolean)): (Symbol, RelValue[_]) = (a._1, BooleanValue(a._2))

  IntValue.ops(":+")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a + b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops(":-")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a - b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops(":*")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a * b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops(":/")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a / b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops(":<")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    BooleanValue(a < b.asInstanceOf[IntValue].wrapped)
  }
  StringValue.ops(":<")(classOf[StringValue]) = (a: String, b: RelValue[_]) => {
    BooleanValue(a.compareTo(b.asInstanceOf[StringValue].wrapped) < 0)
  }
  StringValue.ops(":+")(classOf[StringValue]) = (a: String, b: RelValue[_]) => {
    StringValue(a + b.asInstanceOf[StringValue].wrapped)
  }
  BooleanValue.ops("&&")(classOf[BooleanValue]) = (a: Boolean, b: RelValue[_]) => {
    BooleanValue(a && b.asInstanceOf[BooleanValue].wrapped)
  }
  BooleanValue.ops("||")(classOf[BooleanValue]) = (a: Boolean, b: RelValue[_]) => {
    BooleanValue(a || b.asInstanceOf[BooleanValue].wrapped)
  }
}

object main extends RelEnv {
  //implicit def Symbol2String(a: Symbol):String = a.toString()
  //implicit def TupleSym2TupleStr(a: (Symbol, Any)): (String, Any) = (a._1.toString(), a._2)
  //implicit def TupleSymFct2TupleStrFct(a: (Symbol, (Any)=>Any)): (String, (Any)=>Boolean) = (a._1.toString(), a._2.asInstanceOf[(Any)=>Boolean])

  def main(args: Array[String]) {
    val A = new Relation('id, 'fname)
      .add('id-> 0, 'fname-> "Maxime")
      .add('id-> 1, 'fname-> "Jérôme")
      .add('id-> 1, 'fname-> "Jérôme") // removed

    val B = new Relation('id, 'lname)
      .add('id-> 0, 'lname-> "Istasse")
      .add('id-> 1, 'lname-> "Lemaire")

    /*println(A.join(B).extend('id, 'lname)('hashfname, 'hashlname, 'product){
      (fname:Int, lname: String) => {Seq(
        fname.hashCode,
        lname.hashCode,
        fname.hashCode*lname.hashCode
      )}}
    )
    println(
      A.join(B).extend('id, 'lname)('hash){
        (id:Int, lname: String) => {Seq(
          3
        )}}.project('hash).rename('hash -> 'lol)
    )*/
    //println('hash := 'ok)

    val students = new Relation('id, 'name, 'surname)
      .add('id->0, 'name->"Jérôme", 'surname->"Lemaire")
      .add('id->1, 'name->"Maxime", 'surname->"Istasse")
      .add('id->2, 'name->"Charles", 'surname->"Jacquet")

    val midterm = new Relation('id, 'points)
      .add('id->0, 'points->20)
      .add('id->1, 'points->10)
      .add('id->2, 'points->15)

    println(
      (students join midterm)
        .extend('normal) {
          'normal := ('points :<> max('points)) && ('points :<> min('points))
        }
        where {'points :< max('points)}
        project ('id, 'name, 'surname)
        rename('name as 'prénom,'surname as 'nom)
    )

    println(new Relation('A, 'B)
        .add('A->new Relation('id, 'name).add('id->0, 'name->"Maxime").add('id->1, 'name->"Jérôme"),
              'B->new Relation('id, 'surname).add('id->0, 'surname->"Istasse").add('id->1, 'surname->"Lemaire"))
        .extend('join){
          'join := ('A join 'B).extend('concat){
            'concat := 'name :+ ">>>" :+ 'surname
          }
        }
    )
    println(dum == dee)
  }
}
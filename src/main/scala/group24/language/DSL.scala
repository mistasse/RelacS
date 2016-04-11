package group24.language

import group24.library.{Relation => RRelation, _}
import Types._

/**
  * Created by mistasse on 6/04/16.
  */

class Assignment(val sym: Symbol, val clos: (Types.MutableRecord)=>Unit)

class Environment(val offsets: Offsets) {
  def this() = this(null)
}

class RenameAs(a: Symbol) {
  def as(b: Symbol): (Symbol, Symbol) = (a, b)
}

object RELATION {
  def apply(syms: Symbol*): RRelation = new RRelation(syms:_*)
}

class RELATION(val rel: RRelation) {

  def &(values: RelValueWrapper*): RRelation = {
    rel.addAndCheckConstraints(values.map(_.wrapped))
    return rel
  }

  def &(values: Evaluated*): Evaluated = {
    new ClosureEvaluated((rec: Record) => {
      val evaluated = values.map(_(rec))
      new RelationValue(rel.join(dee).addAndCheckConstraints(evaluated))
    })
  }

  def JOIN(other: RRelation): RRelation = rel.join(other)
  def RENAME(renamings: (Symbol, Symbol)*): RRelation = rel.rename(renamings:_*)
  def PROJECT(keep: Symbol*): RRelation = rel.project(keep:_*)

  def WHERE(bool: Evaluated*)(implicit renv: Ref[Environment]): RRelation = {
    val previous_env = renv.get
    renv.set(new Environment(rel.offsets))
    val ret = rel.where(bool:_*)
    renv.set(previous_env)
    return ret
  }

  def EXTEND(assignments: Assignment*)(implicit renv: Ref[Environment]): RRelation = {
    val previous_env = renv.get
    val syms = assignments.map(_.sym)
    val fcts = assignments.map(_.clos)
    renv.set(new Environment(new Offsets(rel.header ++ syms)))
    val ret = rel.extend(syms:_*)((rec) => fcts.foreach(fct => fct(rec)))
    renv.set(previous_env)
    return ret
  }

  def SHOW(): Unit = {
    println(rel)
  }

}

class RELATIONEval(val wrapped: Evaluated) extends Evaluated {
  def apply(rec: Record): RelValue[_] = wrapped(rec)

  def JOIN(other: Evaluated): Evaluated = wrapped join other
  def RENAME(renamings: (Symbol, Symbol)*): Evaluated = wrapped rename(renamings:_*)
  def PROJECT(keep: Symbol*): Evaluated = wrapped project(keep:_*)

  def WHERE(bool: Evaluated*)(implicit renv: Ref[Environment]): Evaluated = wrapped where(bool:_*)
  def EXTEND(assignments: Assignment*)(implicit renv: Ref[Environment]): Evaluated = wrapped extend(assignments:_*)
}

trait RelEnv {
  implicit val renv: Ref[Environment] = new ThreadSafeRef[Environment](new Environment())

  implicit def Sym2Identifier(a: Symbol)(implicit renv: Ref[Environment]): Identifier = new Identifier(a)
  implicit def SymbolAsSymbol(a: Symbol): RenameAs = new RenameAs(a)

  implicit def SymRel2RelValue(a: (Symbol, RRelation)): (Symbol, RelValue[_]) = (a._1, new RelationValue(a._2))
  implicit def Rel2Evaluated(a: RRelation): Evaluated = new RelValueWrapper(new RelationValue(a))
  implicit def Rel2RELATION(a: RRelation): RELATION = new RELATION(a)
  implicit def Evaluated2RELATIONEval(a: Evaluated): RELATIONEval = new RELATIONEval(a)

  implicit def SymString2RelValue(a: (Symbol, String)): (Symbol, RelValue[_]) = (a._1, StringValue(a._2))
  implicit def String2Evaluated(a: String): RelValueWrapper = new RelValueWrapper(StringValue(a))

  implicit def SymInt2RelValue(a: (Symbol, Int)): (Symbol, RelValue[_]) = (a._1, IntValue(a._2))
  implicit def Int2Evaluated(a: Int): RelValueWrapper = new RelValueWrapper(IntValue(a))

  implicit def SymBool2RelValue(a: (Symbol, Boolean)): (Symbol, RelValue[_]) = (a._1, BooleanValue(a._2))
  implicit def Boolean2Evaluated(a: Boolean): RelValueWrapper = new RelValueWrapper(BooleanValue(a))

  IntValue.ops("+")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a + b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops("-")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a - b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops("*")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a * b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops("/")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    IntValue(a / b.asInstanceOf[IntValue].wrapped)
  }
  IntValue.ops("<")(classOf[IntValue]) = (a: Int, b: RelValue[_]) => {
    BooleanValue(a < b.asInstanceOf[IntValue].wrapped)
  }
  StringValue.ops("<")(classOf[StringValue]) = (a: String, b: RelValue[_]) => {
    BooleanValue(a.compareTo(b.asInstanceOf[StringValue].wrapped) < 0)
  }
  StringValue.ops("+")(classOf[StringValue]) = (a: String, b: RelValue[_]) => {
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
  println("xD")
  //implicit def Symbol2String(a: Symbol):String = a.toString()
  //implicit def TupleSym2TupleStr(a: (Symbol, Any)): (String, Any) = (a._1.toString(), a._2)
  //implicit def TupleSymFct2TupleStrFct(a: (Symbol, (Any)=>Any)): (String, (Any)=>Boolean) = (a._1.toString(), a._2.asInstanceOf[(Any)=>Boolean])

  def main(args: Array[String]) {

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

    val student =
      RELATION('id, 'name, 'surname) &
        (0, "Maxime", "Istasse") &
        (1, "Jérôme", "Lemaire") &
        (2, "Léonard", "Julémont")

    val grades =
      RELATION('id, 'points) &
        (0, 15) &
        (1, 20)

    (
      (student JOIN grades)
      EXTEND (
        'winner := ('points :== 20),
        'looser := ('points :< 20)
        )
      EXTEND (
        'winning := RELATION('win, 'lose) WHERE('win :== true) RENAME('win as 'lol) EXTEND(),
        'bitch := 3 :+ 5
        )
      WHERE(
        'bitch :== 8
        )

      ) SHOW
  /*
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
    */
  }
}
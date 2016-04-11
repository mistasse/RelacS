package group24.language

import group24.library._

import scala.collection.mutable

/**
  * Created by mistasse on 6/04/16.
  */

object Types {
  type Evaluated = (Seq[RelValue[_]])=>RelValue[_]
  type Record = Seq[RelValue[_]]
}

trait RequestWrapper {
  def :==(other: Types.Evaluated): Types.Evaluated
  def :<(other: Types.Evaluated): Types.Evaluated
  def :<=(other: Types.Evaluated): Types.Evaluated
  def :>(other: Types.Evaluated): Types.Evaluated
  def :>=(other: Types.Evaluated): Types.Evaluated
  def :>=(other: Types.Evaluated): Types.Evaluated
}

class Identifier(sym: Symbol)(implicit renv: Ref[Environment]) extends Types.Evaluated {

  def :=(other: Types.Evaluated): (Array[RelValue[_]])=>Unit = {
    return (rec: Array[RelValue[_]]) => {
      rec(renv.get.offsets(sym)) = other(rec)
    }
  }

  def idx: Int = renv.get.offsets(sym)

  def apply(rec: Seq[RelValue[_]]): RelValue[_] = rec(idx)

  def :==(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) == other(rec)
  def :<(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) < other(rec)
  def :<=(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) <= other(rec)
  def :>(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) > other(rec)
  def :>=(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) >= other(rec)
  def :+(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) + other(rec)
  def :-(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) - other(rec)
  def :*(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) * other(rec)
  def :/(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) / other(rec)
  def &&(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) && other(rec)
  def ||(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) || other(rec)

  def JOIN(other: Types.Evaluated): Types.Evaluated = (rec) => rec(idx) join other(rec)

  def as(other: Symbol): (Symbol, Symbol) = {
    return (sym, other)
  }
}

class RelValueWrapper(val value: RelValue[_]) extends Types.Evaluated {
  def apply(rec: Types.Record): RelValue[_] = value

  def :==(other: Types.Evaluated): Types.Evaluated = (rec) => value == other(rec)
  def :<(other: Types.Evaluated): Types.Evaluated = (rec) => value < other(rec)
  def :<=(other: Types.Evaluated): Types.Evaluated = (rec) => value <= other(rec)
  def :>(other: Types.Evaluated): Types.Evaluated = (rec) => value > other(rec)
  def :>=(other: Types.Evaluated): Types.Evaluated = (rec) => value >= other(rec)
  def :+(other: Types.Evaluated): Types.Evaluated = (rec) => value + other(rec)
  def :-(other: Types.Evaluated): Types.Evaluated = (rec) => value - other(rec)
  def :*(other: Types.Evaluated): Types.Evaluated = (rec) => value * other(rec)
  def :/(other: Types.Evaluated): Types.Evaluated = (rec) => value / other(rec)
  def &&(other: Types.Evaluated): Types.Evaluated = (rec) => value && other(rec)
  def ||(other: Types.Evaluated): Types.Evaluated = (rec) => value || other(rec)
}

class Environment(val offsets: Offsets) {
  def this() = this(null)
}

object REL {
  def apply(rel: Relation): REL = new REL(rel)
}

class REL(val rel: Relation) {
  def this(syms: Symbol*) = this(new Relation(syms:_*))
  def &(values: RelValue[_]*): Relation = {
    rel.addAndCheckConstraints(values)
    return rel
  }

  def WHERE(bool: Types.Evaluated)(implicit renv: Ref[Environment]): Relation = {
    val previous_env = renv.get
    renv.set(new Environment(rel.offsets))
    val ret = rel.where(bool)
    renv.set(previous_env)
    return ret
  }

  def EXTEND(syms: Symbol*)(closures: ((Array[RelValue[_]])=>Unit)*)(implicit renv: Ref[Environment]): Relation = {
    val previous_env = renv.get
    renv.set(new Environment(new Offsets(rel.header ++ syms)))
    val ret = rel.extend(syms:_*)((rec) => closures.foreach(c => c(rec)))
    renv.set(previous_env)
    return ret
  }

}

/*
object max {
  def apply(attribute: Symbol)(implicit renv: Ref[Environment]): group24.core.RelValue[_] = apply(renv.get.rel, attribute)
  def apply(rel: group24.core.RelValue[_], attribute: Symbol)(implicit renv: Ref[Environment]): group24.core.RelValue[_] = apply(rel.asInstanceOf[group24.core.RelationValue].wrapped, attribute)
  def apply(rel: Relation, attribute: Symbol): group24.core.RelValue[_] = {
    var max: Option[group24.core.RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(attribute))
      if(max.isEmpty)
        max = Some(current)
      else if(current.:>(max.get) == group24.core.BooleanValue.TRUE)
        max = Some(current)
    }
    return max.get
  }
}

object min {
  def apply(attribute: Symbol)(implicit renv: Ref[Environment]): group24.core.RelValue[_] = apply(renv.get.rel, attribute)
  def apply(rel: group24.core.RelValue[_], attribute: Symbol)(implicit renv: Ref[Environment]): group24.core.RelValue[_] = apply(rel.asInstanceOf[group24.core.RelationValue].wrapped, attribute)
  def apply(rel: Relation, attribute: Symbol): group24.core.RelValue[_] = {
    var min: Option[group24.core.RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(attribute))
      if(min.isEmpty)
        min = Some(current)
      else if(current.:<(min.get) == group24.core.BooleanValue.TRUE)
        min = Some(current)
    }
    return min.get
  }
}
*/

trait RelEnv {
  implicit val renv: Ref[Environment] = new ThreadSafeRef[Environment](new Environment())

  implicit def Sym2Identifier(a: Symbol)(implicit renv: Ref[Environment]): Identifier = new Identifier(a)

  implicit def Rel2RELATION(a: Relation): REL = REL(a)
  implicit def Rel2Evaluated(a: Relation): Types.Evaluated = (rec: Types.Record) => a
  implicit def Rel2RelValue(a: Relation): RelValue[_] = new RelationValue(a)
  implicit def SymRel2RelValue(a: (Symbol, Relation)): (Symbol, RelValue[_]) = (a._1, new RelationValue(a._2))

  implicit def String2Evaluated(a: String): RelValueWrapper = new RelValueWrapper(StringValue(a))
  implicit def String2RelValue(a: String): RelValue[_] = StringValue(a)
  implicit def SymString2RelValue(a: (Symbol, String)): (Symbol, RelValue[_]) = (a._1, StringValue(a._2))

  implicit def Int2Evaluated(a: Int): RelValueWrapper = new RelValueWrapper(IntValue(a))
  implicit def Int2RelValue(a: Int): RelValue[_] = IntValue(a)
  implicit def SymInt2RelValue(a: (Symbol, Int)): (Symbol, RelValue[_]) = (a._1, IntValue(a._2))

  implicit def Boolean2Evaluated(a: Boolean): RelValueWrapper = new RelValueWrapper(BooleanValue(a))
  implicit def Bool2RelValue(a: Boolean): RelValue[_] = BooleanValue(a)
  implicit def SymBool2RelValue(a: (Symbol, Boolean)): (Symbol, RelValue[_]) = (a._1, BooleanValue(a._2))

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

    println(students.EXTEND('ok, 'xD)('ok := 3 :+ 3, 'xD := 3))

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
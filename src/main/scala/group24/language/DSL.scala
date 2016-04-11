package group24.language

import group24.library.{Relation => RRelation, _}

import scala.collection.mutable

/**
  * Created by mistasse on 6/04/16.
  */

object Types {
  /**
    * Evaluated is
    */
  type Evaluated = (Seq[RelValue[_]])=>RelValue[_]
  /**
    * Record is the format of a tuple in a relation
    */
  type Record = Seq[RelValue[_]]
  /**
    * MutableRecord are used just when extending relations
    */
  type MutableRecord = Array[RelValue[_]]
}

class Assignment(val sym: Symbol, val clos: (Types.MutableRecord)=>Unit)

class Identifier(sym: Symbol)(implicit renv: Ref[Environment]) extends Types.Evaluated {

  def :=(other: Types.Evaluated): Assignment = {
    new Assignment(sym, (rec: Array[RelValue[_]]) => {
      rec(renv.get.offsets(sym)) = other(rec)
    })
  }

  @inline def idx: Int = renv.get.offsets(sym)

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

object RELATION {
  def apply(rel: RRelation): RRelation = rel
  def apply(syms: Symbol*): RRelation = new RRelation(syms:_*)
}

class RELATION(val rel: RRelation) {
  def &(values: RelValue[_]*): RRelation = {
    rel.addAndCheckConstraints(values)
    return rel
  }

  def JOIN(other: RRelation): RRelation = rel.join(other)

  def WHERE(bool: Types.Evaluated)(implicit renv: Ref[Environment]): RRelation = {
    val previous_env = renv.get
    renv.set(new Environment(rel.offsets))
    val ret = rel.where(bool)
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

  implicit def Rel2RELATION(a: RRelation): RELATION = new RELATION(a)
  implicit def Rel2Evaluated(a: RRelation): Types.Evaluated = (rec: Types.Record) => a
  implicit def Rel2RelValue(a: RRelation): RelValue[_] = new RelationValue(a)
  implicit def SymRel2RelValue(a: (Symbol, RRelation)): (Symbol, RelValue[_]) = (a._1, new RelationValue(a._2))

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

    println(
      (student JOIN grades)
      EXTEND(
        'winner := ('points :== 20),
        'looser := ('points :< 20)
      )
    )
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
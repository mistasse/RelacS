package group24.language

import group24.library.{Relation => RRelation, _}
import Types._

import scala.collection.mutable.HashMap

/**
  * Created by mistasse on 6/04/16.
  */

class Assignment(val sym: Symbol, val clos: (Types.MutableRecord)=>Unit)

class Environment(val hashMap: HashMap[Symbol, RelValue[_]], val current_relation: RRelation, val new_offsets: Offsets=null) {
  def this(env: Environment, rel: RRelation) = this(env.hashMap.clone(), rel, null)
  def this(env: Environment, rel: RRelation, new_offsets: Offsets) = this(env.hashMap.clone(), rel, new_offsets)
}

class RenameAs(a: Symbol) {
  def as(b: Symbol): (Symbol, Symbol) = (a, b)
  def AS(b: Symbol): (Symbol, Symbol) = (a, b)
}

object RELATION {
  /**
    * Allows to write RELATION('header1, 'header2), just like in Rel
    */
  def apply(syms: Symbol*): RRelation = new RRelation(syms:_*)

  /**
    * Factorized code for contextualized WHERE
    */
  def WHERE(rel: RRelation, bool: Evaluated*)(implicit renv: Ref[Environment]): RRelation = {
    val parent_env = renv.get
    val env = new Environment(parent_env, rel)
    renv.set(env)
    val map = env.hashMap

    val ret = rel.where((rec) => {
      for((h, i) <- rel.offsets())
        map.put(h, rec(i))
      BooleanValue(bool.forall(fct => fct(env) equals BooleanValue.TRUE))
    })
    renv.set(parent_env)
    return ret
  }

  /**
    * Factorized code for contextualized EXTEND
    */
  def EXTEND(rel: RRelation, assignments: Assignment*)(implicit renv: Ref[Environment]): RRelation = {
    val previous_env = renv.get
    val syms = assignments.map(_.sym)
    val fcts = assignments.map(_.clos)
    val env = new Environment(previous_env, rel, new Offsets(rel.header ++ syms))
    renv.set(env)
    val map = env.hashMap
    val ret = rel.extend(syms:_*)((rec) =>{
      for((h, i) <- rel.offsets())
        map.put(h, rec(i))
      fcts.foreach(fct => fct(rec))
    })
    renv.set(previous_env)
    return ret
  }
}

/**
  * Allows to use caps in the main parts of the program, or static parts of closures, and add records with &
  * E.G:
  * EXTEND(
  *   'ok := RELATION('hey) & (3)
  * )
  */
class RELATION(val rel: RRelation) {

  /**
    * Static, preferred since more specific than Evaluated*
    */
  def &(values: RelValueWrapper*): RRelation = {
    rel.addAndCheckConstraints(values.map(_.wrapped))
    return rel
  }

  /**
    * Allows to add dynamic fields
    * E.G:
    * EXTEND(
    *   'ok := RELATION('hey) & ('existingField)
    * )
    */
  def &(values: Evaluated*): Evaluated = {
    new ClosureEvaluated((env: Environment) => {
      val evaluated = values.map(_(env))
      new RelationValue(rel.clone().addAndCheckConstraints(evaluated))
    })
  }

  def JOIN(other: RRelation): RRelation = rel.join(other)
  def RENAME(renamings: (Symbol, Symbol)*): RRelation = rel.rename(renamings:_*)
  def PROJECT(keep: Symbol*): RRelation = rel.project(keep:_*)

  def WHERE(bool: Evaluated*)(implicit renv: Ref[Environment]): RRelation = RELATION.WHERE(rel, bool:_*)
  def EXTEND(assignments: Assignment*)(implicit renv: Ref[Environment]): RRelation = RELATION.EXTEND(rel, assignments:_*)
  def PRINT(): RRelation = {
    System.out.println(rel)
    return rel
  }
}

/**
  * Allows to use caps within expressions
  */
class RELATIONEval(val wrapped: Evaluated) {
  def apply(env: Environment): RelValue[_] = wrapped(env)

  def &(values: Evaluated*): Evaluated = {
    new ClosureEvaluated((env: Environment) => {
      val evaluated = values.map(_(env))
      new RelationValue(this(env).wrapped.asInstanceOf[RRelation].clone().addAndCheckConstraints(evaluated))
    })
  }

  def JOIN(other: Evaluated): Evaluated = wrapped join other
  def RENAME(renamings: (Symbol, Symbol)*): Evaluated = wrapped rename(renamings:_*)
  def PROJECT(keep: Symbol*): Evaluated = wrapped project(keep:_*)

  def WHERE(bool: Evaluated*)(implicit renv: Ref[Environment]): Evaluated = wrapped where(bool:_*)
  def EXTEND(assignments: Assignment*)(implicit renv: Ref[Environment]): Evaluated = wrapped extend(assignments:_*)
}

trait RelEnv {
  implicit val renv: Ref[Environment] = new ThreadSafeRef[Environment](new Environment(HashMap.empty[Symbol, RelValue[_]], null))

  // Allows creating identifiers linked to the current Environment Reference for further evaluation
  implicit def Sym2Identifier(a: Symbol)(implicit renv: Ref[Environment]): Identifier = new Identifier(a)
  implicit def SymbolAsSymbol(a: Symbol): RenameAs = new RenameAs(a)

  // The trickiest part: Why only those? And all those ones?
  // 1) Here, we allow syntactic sugar
  implicit def Rel2aRELATION(a: RRelation): RELATION = new RELATION(a)
  // 2) Within some blocks, we want to do EXTEND('x := RELATION(...))
  implicit def Rel2Evaluated(a: RRelation): RelValueWrapper = new RelValueWrapper(new RelationValue(a))
  // 3) Here, we allow the inner relations to accept more operators, ... while keeping the same syntax
  implicit def Evaluated2RELATIONEval(a: Evaluated): RELATIONEval = new RELATIONEval(a)
  // Notice: We had to have Evaluated's method in lowercase, because otherwise, there were conflicts implicit conversions
  // at the top level(Relation could either be caster to Evaluated or RELATION). This is a nice trick to allow the same
  // syntax!

  // Obvious here!
  implicit def String2Evaluated(a: String): RelValueWrapper = new RelValueWrapper(StringValue(a))
  implicit def Int2Evaluated(a: Int): RelValueWrapper = new RelValueWrapper(IntValue(a))
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

object COUNT {
  def apply()(implicit renv: Ref[Environment]): Evaluated = CE((rec) => this(renv.get.current_relation))
  def apply(rel: Evaluated): Evaluated = CE((rec) => this(rel(rec).wrapped.asInstanceOf[RRelation]))
  def apply(rel: RRelation): RelValue[_] = {
    new IntValue(rel.size)
  }
}

object MIN {
  def apply(sym: Symbol)(implicit renv: Ref[Environment]): Evaluated = CE((rec) => this(renv.get.current_relation, sym))
  def apply(rel: Evaluated, sym: Symbol): Evaluated = CE((rec) => this(rel(rec).wrapped.asInstanceOf[RRelation], sym))
  def apply(rel: RRelation, sym: Symbol): RelValue[_] = {
    var min: Option[RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(sym))
      if(min.isEmpty)
        min = Some(current)
      else if((current < min.get) equals BooleanValue.TRUE)
        min = Some(current)
    }
    min.get
  }
}

object MAX {
  def apply(sym: Symbol)(implicit renv: Ref[Environment]): Evaluated = CE((rec) => this(renv.get.current_relation, sym))
  def apply(rel: Evaluated, sym: Symbol): Evaluated = CE((rec) => this(rel(rec).wrapped.asInstanceOf[RRelation], sym))
  def apply(rel: RRelation, sym: Symbol): RelValue[_] = {
    var max: Option[RelValue[_]] = None
    for(record <- rel.records) {
      val current = record(rel.offsets(sym))
      if(max.isEmpty)
        max = Some(current)
      else if((current > max.get) equals BooleanValue.TRUE)
        max = Some(current)
    }
    max.get
  }
}

object main extends RelEnv {

  def main(args: Array[String]) {

    val student =
      RELATION('id, 'name, 'surname) &
        (0, "Maxime", "Istasse") &
        (1, "Jérôme", "Lemaire") &
        (2, "Léonard", "Julémont")

    val grades =
      RELATION('id, 'points) &
        (0, 15) &
        (1, 18) &
        (2, 20)

    (
      (student JOIN grades)
      EXTEND (
        'best := ('points :== MAX('points)),
        'better_than := student join grades
          rename('points as 'otherp)
          where('points :> 'otherp)
          project('name, 'surname)
        )
      PRINT
    )
  }
}
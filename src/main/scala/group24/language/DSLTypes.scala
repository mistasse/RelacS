package group24.language

import group24.library.{Relation => RRelation, RelationValue, Ref, RelValue}

/**
  * Created by mistasse on 11/04/16.
  */

object Types {
  /**
    * Evaluated is
    */
  type Evaluated = (Environment)=>RelValue[_]
  /**
    * Record is the format of a tuple in a relation
    */
  type Record = Seq[RelValue[_]]
  /**
    * MutableRecord are used just when extending relations
    */
  type MutableRecord = Array[RelValue[_]]
}

object CE {
  def apply(clos: Types.Evaluated) = new ClosureEvaluated(clos)
}

/**
  * Every closure is associated to an instance
  * So it is possible to chain operators on "Evaluated"
  */
trait Evaluated extends Types.Evaluated {
  def apply(env: Environment): RelValue[_]

  def :==(other: Evaluated): Evaluated = CE((env) => this(env) == other(env))
  def :<>(other: Evaluated): Evaluated = CE((env) => this(env) <> other(env))
  def :<(other: Evaluated): Evaluated = CE((env) => this(env) < other(env))
  def :<=(other: Evaluated): Evaluated = CE((env) => this(env) <= other(env))
  def :>(other: Evaluated): Evaluated = CE((env) => this(env) > other(env))
  def :>=(other: Evaluated): Evaluated = CE((env) => this(env) >= other(env))
  def :+(other: Evaluated): Evaluated = CE((env) => this(env) + other(env))
  def :-(other: Evaluated): Evaluated = CE((env) => this(env) - other(env))
  def :*(other: Evaluated): Evaluated = CE((env) => this(env) * other(env))
  def :/(other: Evaluated): Evaluated = CE((env) => this(env) / other(env))
  def &&(other: Evaluated): Evaluated = CE((env) => this(env) && other(env))
  def ||(other: Evaluated): Evaluated = CE((env) => this(env) || other(env))
  def join(other: Evaluated): Evaluated = CE((env) => this(env) join other(env))
  def not_matching(other: Evaluated): Evaluated = CE((env) => this(env) not_matching other(env))
  def union(other: Evaluated): Evaluated = CE((env) => this(env) union other(env))
  def rename(renamings: (Symbol, Symbol)*): Evaluated = CE((env) => this(env) rename(renamings:_*))
  def project(keep: Symbol*): Evaluated = CE((env) => this(env) project(keep:_*))
  def group(selected: Seq[Symbol], attributName: Symbol): Evaluated = CE((env) => this(env)  group(selected, attributName))
  def where(conditions: Evaluated*)(implicit renv: Ref[Environment]): Evaluated = {
    CE((env) => {
      val rel = this(env).asInstanceOf[RelationValue].wrapped
      new RelationValue(RELATION.WHERE(rel, conditions:_*))
    })
  }
  def extend(assignments: Assignment*)(implicit renv: Ref[Environment]): Evaluated = {
    CE((env) => {
      val rel = this(env).asInstanceOf[RelationValue].wrapped
      new RelationValue(RELATION.EXTEND(rel, assignments:_*))
    })
  }

}

/**
  * Chained operators, ...
  */
class ClosureEvaluated(clos: Types.Evaluated) extends Evaluated {
  override def apply(env: Environment): RelValue[_] = clos(env)
}

/**
  * Fields
  */
class Identifier(sym: Symbol)(implicit renv: Ref[Environment]) extends Evaluated {
  def :=(other: Types.Evaluated): Assignment = {
    new Assignment(sym, (rec: Array[RelValue[_]]) => {
      rec(renv.get.new_offsets(sym)) = other(renv.get)
    })
  }

  def apply(env: Environment): RelValue[_] = env.hashMap(sym)
}

/**
  * Constants
  */
class RelValueWrapper(val wrapped: RelValue[_]) extends Evaluated {
  def apply(env: Environment): RelValue[_] = wrapped
}

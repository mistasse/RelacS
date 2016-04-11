package group24.language

import group24.library.{RelationValue, Ref, RelValue}

/**
  * Created by mistasse on 11/04/16.
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

object CE {
  def apply(clos: Types.Evaluated) = new ClosureEvaluated(clos)
}

/**
  * Every closure is associated to an instance
  * So it is possible to chain operators on "Evaluated"
  */
trait Evaluated extends Types.Evaluated {
  def apply(rec: Seq[RelValue[_]]): RelValue[_]

  def :==(other: Evaluated): Evaluated = CE((rec) => this(rec) == other(rec))
  def :<(other: Evaluated): Evaluated = CE((rec) => this(rec) < other(rec))
  def :<=(other: Evaluated): Evaluated = CE((rec) => this(rec) <= other(rec))
  def :>(other: Evaluated): Evaluated = CE((rec) => this(rec) > other(rec))
  def :>=(other: Evaluated): Evaluated = CE((rec) => this(rec) >= other(rec))
  def :+(other: Evaluated): Evaluated = CE((rec) => this(rec) + other(rec))
  def :-(other: Evaluated): Evaluated = CE((rec) => this(rec) - other(rec))
  def :*(other: Evaluated): Evaluated = CE((rec) => this(rec) * other(rec))
  def :/(other: Evaluated): Evaluated = CE((rec) => this(rec) / other(rec))
  def &&(other: Evaluated): Evaluated = CE((rec) => this(rec) && other(rec))
  def ||(other: Evaluated): Evaluated = CE((rec) => this(rec) || other(rec))
  def join(other: Evaluated): Evaluated = CE((rec) => this(rec) join other(rec))
  def rename(renamings: (Symbol, Symbol)*): Evaluated = CE((rec) => this(rec) rename(renamings:_*))
  def project(keep: Symbol*): Evaluated = CE((rec) => this(rec) project(keep:_*))
  def where(conditions: Evaluated*)(implicit renv: Ref[Environment]): Evaluated = {
    CE((rec) => {
      val rel = this(rec).asInstanceOf[RelationValue].wrapped
      new RelationValue(RELATION.WHERE(rel, conditions:_*))
    })
  }
  def extend(assignments: Assignment*)(implicit renv: Ref[Environment]): Evaluated = {
    CE((rec) => {
      val rel = this(rec).asInstanceOf[RelationValue].wrapped
      new RelationValue(RELATION.EXTEND(rel, assignments:_*))
    })
  }
}

/**
  * Chained operators, ...
  */
class ClosureEvaluated(clos: Types.Evaluated) extends Evaluated {
  override def apply(rec: Seq[RelValue[_]]): RelValue[_] = clos(rec)
}

/**
  * Fields
  */
class Identifier(sym: Symbol)(implicit renv: Ref[Environment]) extends Evaluated {
  def :=(other: Types.Evaluated): Assignment = {
    new Assignment(sym, (rec: Array[RelValue[_]]) => {
      rec(renv.get.offsets(sym)) = other(rec)
    })
  }

  @inline def idx: Int = renv.get.offsets(sym)

  def apply(rec: Seq[RelValue[_]]): RelValue[_] = rec(idx)
}

/**
  * Constants
  */
class RelValueWrapper(val wrapped: RelValue[_]) extends Evaluated {
  def apply(rec: Types.Record): RelValue[_] = wrapped
}

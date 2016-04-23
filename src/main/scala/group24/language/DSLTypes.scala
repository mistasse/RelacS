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

class PseudoMonadRecord(val header: Seq[Symbol], val array: Seq[RelValue[_]]) {
  def apply(idx: Symbol): RelValue[_] = array(header.indexOf(idx))
  def apply(idx: Int): RelValue[_] = array(idx)

  override def equals(other: Any): Boolean = {
    if(!other.isInstanceOf[PseudoMonadRecord])
      return false;
    val that = other.asInstanceOf[PseudoMonadRecord]
    if(header.toSet != that.header.toSet)
      return false;
    val mapping = if(header == that.header) Range(0, header.length) else header.map(that.header.indexOf(_))
    array equals mapping.map(that.array);
  }
  override def hashCode(): Int = {
    array.hashCode()
  }
  override def toString(): String = {
    val b = new StringBuilder("{")
    for(i <- array.indices) {
      if(i != 0)
        b.append(", ")
      b.append(header(i).name).append(": ").append(array(i))
    }
    b.append("}").toString()
  }
}

class PseudoMonadRelation(val rel: RRelation) extends Set[PseudoMonadRecord] {
  val records = rel.records.map(new PseudoMonadRecord(rel.header, _))

  override def toString(): String = {
    rel.toString()
  }
  override def contains(elem: PseudoMonadRecord): Boolean = ???
  override def -(elem: PseudoMonadRecord): Set[PseudoMonadRecord] = ???
  override def +(elem: PseudoMonadRecord): Set[PseudoMonadRecord] = {
    val ret = rel.clone()
    val mapping = rel.header.map(elem.header.indexOf(_))
    ret.addAndCheckConstraints(mapping.map(elem(_)))
    return new PseudoMonadRelation(ret)
  }
  override def iterator: Iterator[PseudoMonadRecord] = records.iterator
}

/**
  * Constants
  */
class RelValueWrapper(val wrapped: RelValue[_]) extends Evaluated {
  def apply(env: Environment): RelValue[_] = wrapped
}

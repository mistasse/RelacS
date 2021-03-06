package group24.library

import scala.collection.mutable

/**
  * Created by mistasse on 7/04/16.
  */


abstract class RelValue[T](val wrapped: T) {
  def get[T] = wrapped.asInstanceOf[T]

  def ==(other: RelValue[_]): RelValue[_] = {
    return BooleanValue(wrapped.equals(other.wrapped))
  }

  def <>(other: RelValue[_]): RelValue[_] = {
    return BooleanValue(!wrapped.equals(other.wrapped))
  }

  def +(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("+ not implemented on "+getClass.getName)
  }

  def -(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("- not implemented on "+getClass.getName)
  }

  def *(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("* not implemented on "+getClass.getName)
  }

  def /(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("/ not implemented on "+getClass.getName)
  }

  def &&(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("&& not implemented on "+getClass.getName)
  }

  def ||(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("|| not implemented on "+getClass.getName)
  }

  def <(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("< not implemented on "+getClass.getName)
  }

  def <=(other: RelValue[_]): RelValue[_] = BooleanValue((this.<(other) equals BooleanValue.TRUE) || (this.==(other) equals BooleanValue.TRUE))
  def >(other: RelValue[_]): RelValue[_] = !BooleanValue(this.<=(other) equals BooleanValue.TRUE)
  def >=(other: RelValue[_]): RelValue[_] = !BooleanValue(this.<(other) equals BooleanValue.TRUE)


  /**
    * Now, handle Relvars, have to handle both relvars from the closure and relvars from other places
    */
  def rename(renamings: (Symbol, Symbol)*): RelValue[_] = {
    throw new RuntimeException("rename not implemented on "+getClass.getName)
  }
  def project(kept: Symbol*): RelValue[_] = {
    throw new RuntimeException("project not implemented on "+getClass.getName)
  }

  def join(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("join not implemented on "+getClass.getName)
  }

  def not_matching(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("not_matching not implemented on "+getClass.getName)
  }

  def union(other: RelValue[_]): RelValue[_] = {
    throw new RuntimeException("union not implemented on "+getClass.getName)
  }
  def group(selected: Seq[Symbol], attributName: Symbol): RelValue[_] = {
    throw new RuntimeException("group not implemented on "+getClass.getName)
  }

  override def toString(): String = wrapped.toString()

  override def hashCode(): Int = wrapped.hashCode()

  override def equals(a: Any): Boolean = if(a.isInstanceOf[RelValue[_]]) wrapped.equals(a.asInstanceOf[RelValue[_]].wrapped) else false
}

class RelationValue(wrapped: Relation) extends RelValue[Relation](wrapped) {
  override def rename(renamings: (Symbol, Symbol)*): RelValue[_] = {
    return new RelationValue(wrapped.rename(renamings:_*))
  }
  override def project(kept: Symbol*): RelValue[_] = {
    return new RelationValue(wrapped.project(kept:_*))
  }
  override def join(other: RelValue[_]): RelValue[_] = {
    return new RelationValue(wrapped.join(other.asInstanceOf[RelValue[Relation]].wrapped))
  }
  override def not_matching(other: RelValue[_]): RelValue[_] = {
    return new RelationValue(wrapped.not_matching(other.asInstanceOf[RelValue[Relation]].wrapped))
  }
  override def union(other: RelValue[_]): RelValue[_] = {
    return new RelationValue(wrapped.union(other.asInstanceOf[RelValue[Relation]].wrapped))
  }
  override def group(grouped: Seq[Symbol], as: Symbol): RelValue[_] = {
    return new RelationValue(wrapped.group(grouped, as))
  }

}

object ExtensibleValue {
  val handledOps = Seq("+", "-", "*", "/", "&&", "||", "<")
  def initMap[T](): mutable.HashMap[String, mutable.HashMap[Class[_ <: RelValue[_]], (T, RelValue[_])=>RelValue[_]]] = {
    val ret = new mutable.HashMap[String, mutable.HashMap[Class[_ <: RelValue[_]], (T, RelValue[_])=>RelValue[_]]]();
    for(op <- handledOps) {
      ret(op) = new mutable.HashMap[Class[_ <: RelValue[_]], (T, RelValue[_])=>RelValue[_]]()
    }
    return ret
  }
}

abstract class ExtensibleValue[T](wrapped:T) extends RelValue[T](wrapped) {
  def ops : mutable.HashMap[String, mutable.HashMap[Class[_ <: RelValue[_]], (T, RelValue[_])=>RelValue[_]]]

  @inline def find_op(op: String, klass: Class[_ <: RelValue[_]]): (T, RelValue[_])=>RelValue[_] = ops(op)(klass)

  override def +(other: RelValue[_]): RelValue[_] = find_op("+", other.getClass)(this.wrapped, other)
  override def -(other: RelValue[_]): RelValue[_] = find_op("-", other.getClass)(this.wrapped, other)
  override def *(other: RelValue[_]): RelValue[_] = find_op("*", other.getClass)(this.wrapped, other)
  override def /(other: RelValue[_]): RelValue[_] = find_op("/", other.getClass)(this.wrapped, other)
  override def &&(other: RelValue[_]): RelValue[_] = find_op("&&", other.getClass)(this.wrapped, other)
  override def ||(other: RelValue[_]): RelValue[_] = find_op("||", other.getClass)(this.wrapped, other)
  override def <(other: RelValue[_]): RelValue[_] = find_op("<", other.getClass)(this.wrapped, other)
}

object IntValue {
  val ops = ExtensibleValue.initMap[Int]()

  def apply(wrapped: Int): IntValue = new IntValue(wrapped)
}
class IntValue(wrapped: Int) extends ExtensibleValue[Int](wrapped) {
  def ops = IntValue.ops
}

object StringValue {
  val ops = ExtensibleValue.initMap[String]()

  def apply(wrapped: String): StringValue = new StringValue(wrapped)
}
class StringValue(wrapped: String) extends ExtensibleValue[String](wrapped) {
  def ops = StringValue.ops
}

object BooleanValue {
  val ops = ExtensibleValue.initMap[Boolean]()
  val TRUE = new BooleanValue(true)
  val FALSE = new BooleanValue(false)

  def apply(wrapped: Boolean): BooleanValue = if(wrapped) TRUE else FALSE
}
class BooleanValue(wrapped: Boolean) extends ExtensibleValue[Boolean](wrapped) {
  def ops = BooleanValue.ops

  def unary_!(): BooleanValue = if(wrapped) BooleanValue.FALSE else BooleanValue.TRUE
}

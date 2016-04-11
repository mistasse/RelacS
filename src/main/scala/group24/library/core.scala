package group24.library

import java.util.function.Supplier

import group24.library.RelValue

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

/**
  * Created by mistasse on 6/04/16.
  */

object dum extends Relation()
object dee extends Relation() { add() }

/**
  * The offset class is the mapping for the indices, of titles, titles of indices, and an iterator over both
  *
  * @param header
  */
class Offsets(header: Seq[Symbol]) {
  val dict = new HashMap[Symbol, Int]()
  for (i <- 0 until header.size)
    dict(header(i)) = i

  def apply(h: Symbol): Int = {
    return dict(h)
  }

  def apply(i: Int): Symbol = {
    return header(i)
  }

  def apply(): Iterator[(Symbol, Int)] = {
    return dict.iterator
  }
}

abstract class Ref[T] {
  def get: T
  def set(n: T): Unit
}

class SimpleRef[T](var obj: T) extends Ref[T] {
  def get = obj
  def set(n: T) = obj = n
}

class ThreadSafeRef[T](obj: =>T) extends Ref[T] {
  val local = ThreadLocal.withInitial(new Supplier[T] {def get = obj})

  override def get = local.get()
  override def set(n: T) = local.set(n)
}

class Relation(val header: Seq[Symbol], val offsets: Offsets, var records: HashSet[Seq[RelValue[_]]], var frozen: Boolean) {
  def this(header: Seq[Symbol], offsets:Offsets, records:HashSet[Seq[RelValue[_]]]) =  this(header, offsets, records, true)
  def this(header: Seq[Symbol], records:HashSet[Seq[RelValue[_]]])                  =  this(header, new Offsets(header), records, true)
  def this(header: Seq[Symbol], offsets:Offsets)                                    =  this(header, offsets, new HashSet[Seq[RelValue[_]]](), false)
  def this(header: Symbol*)                                                         =  this(header, new Offsets(header), new HashSet[Seq[RelValue[_]]](), false)

  def arity: Int = this.header.length
  def size: Int = this.records.size

  /**
    *
    * @param dic
    */
  def add(dic: (Symbol, RelValue[_])*): Relation = {
    val record = Array.ofDim[RelValue[_]](dic.length)
    for(i <- 0 until arity)
      record(offsets(dic(i)._1)) = dic(i)._2
    if(frozen) {
      records = records.clone()
      frozen = false
    }
    addAndCheckConstraints(record)
    return this
  }

  def addAndCheckConstraints(record: Seq[RelValue[_]]): Relation = {
    // TODO
    records.add(record)
    return this
  }

  /**
    * Join
    *
    * @param other
    * @return
    */
  def join(other: Relation): Relation = {
    val common = Set(this.header:_*).intersect(Set(other.header:_*))
    val kept = (0 until other.arity).flatMap(i => if(common.contains(other.offsets(i))) None else Some(i))

    val header = this.header ++ other.header.filter(!common.contains(_))
    val ret = new Relation(header:_*)

    for(record_a <- this.records) {
      for(record_b <- other.records) {
        if(common.forall(h => record_a(this.offsets(h)) == record_b(other.offsets(h)))) {
          val mixed = record_a ++ kept.map(i => record_b(i))
          ret.records.add(mixed)
        }
      }
    }

    return ret
  }

  def project(kept: Symbol*): Relation = {
    val ret = new Relation(kept:_*)
    val old_to_new = kept.map(this.offsets(_)) // new(:) = old(old_to_new)

    for(record <- this.records) {
      val new_record = old_to_new.map(record(_))
      ret.records.add(new_record)
    }

    return ret
  }

  def rename(renamings: (Symbol, Symbol)*): Relation = {
    val trans = new HashMap[Symbol, Symbol]()
    renamings.foreach(r => trans(r._1) = r._2)

    val header = this.header.map(h => trans.getOrElse(h, h))

    val ret = new Relation(header, this.records)

    return ret
  }

  def where(f: (Seq[RelValue[_]]=>RelValue[_])): Relation = {
    val ret = new Relation(header, offsets)

    for(record <- this.records) {
      if(f(record).asInstanceOf[RelValue[Boolean]].wrapped)
        ret.records.add(record)
    }

    return ret
  }

  /**
    * extend
    *
    * @param added
    * @return
    */
  def extend(added: Symbol*)(f: (Array[RelValue[_]])=>Unit): Relation = {
    val header = this.header ++ added
    val ret = new Relation(header:_*)

    for(record <- this.records) {
      val new_record = Array.ofDim[RelValue[_]](header.length)
      Array.copy(record.toArray, 0, new_record, 0, record.length)
      f(new_record)
      ret.addAndCheckConstraints(new_record)
    }

    return ret
  }

  /*
  def extend0(mapping: Symbol*)(features: (Symbol, Any)*): Relation = {
    val header = this.header ++ features.map(f => f._1)
    val ret = new Relation(header:_*)

    val methods = Array.ofDim[(Seq[Object])=>Any](features.length)
    for(i <- 0 until methods.length) {
      val obj = features(i)._2
      val method = obj.getClass().getMethods()
        .filter(m => (m.getName == "apply" && m.getParameterTypes().forall(p => p == classOf[Object])))
      methods(i) = (args: Seq[Object]) => method(0).invoke(obj, args:_*)  // MethodHandles.lookup().unreflect(method(0)).bindTo(obj)
    }
    for(record <- this.records) {
      val args = mapping.map(h => record(offsets(h)))
      val new_record = record ++ methods.map(m => m(args.asInstanceOf[Seq[Object]]))
      ret.records += new_record
    }

    return ret
  }

  def extend1(mapping: Symbol*)(added: Symbol*)(gen: Any): Relation = {
    val header = this.header ++ added
    val ret = new Relation(header:_*)

    val method = gen.getClass.getMethods
        .filter(m => (m.getName == "apply" && m.getParameterTypes().forall(p => p == classOf[Object])))(0)
    val apply = (args: Seq[Object]) => method.invoke(gen, args:_*)

    for(record <- this.records) {
      val args = mapping.map(h => record(offsets(h)))
      val new_record = record ++ apply(args.asInstanceOf[Seq[Object]]).asInstanceOf[Seq[group24.core.RelValue[_]]]
      ret.records += new_record
    }

    return ret
  }
  */

  override def toString(): String = {
    val b = new StringBuilder()
    b.append(header.mkString(" ")).append('\n')
    for(record <- records)
      b.append(record.mkString(" ")).append('\n')
    return b.toString()
  }

  override def equals(a: Any): Boolean = {
    if(!a.isInstanceOf[Relation])
      return false
    val other = a.asInstanceOf[Relation]
    if(this.records.size != other.records.size || this.header.toSet != other.header.toSet)
      return false
    val other_to_me = this.header.map(s => other.offsets(s)) // me(:) = other(other_to_me)
    for(record <- other.records) {
      if(!records.contains(other_to_me.map(record)))
        return false
    }
    return true
  }

}
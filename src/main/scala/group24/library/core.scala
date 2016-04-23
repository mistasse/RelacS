package group24.library

import java.util.function.Supplier

import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}

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
  for (i <- header.indices)
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
  override def clone(): Relation = new Relation(header, offsets, records)

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
    addAndCheckConstraints(record)
    return this
  }


  def addAndCheckConstraints(record: Seq[RelValue[_]]): Relation = {
    // TODO
    if(frozen) {
      records = records.clone()
      frozen = false
    }
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
        if(common.forall(h => record_a(this.offsets(h)) equals record_b(other.offsets(h)))) {
          val mixed = record_a ++ kept.map(i => record_b(i))
          ret.records.add(mixed)
        }
      }
    }

    return ret
  }

  def union(other: Relation): Relation = {
    if(this.header.toSet != other.header.toSet)
      throw new RuntimeException("Union of two relations that don't have the same header")

    val header = this.header
    val ret = this.clone()

    val mapping = header.map(other.offsets(_)) // this(0 => 'a) = other(other_offset('a) = mapping(0))
    for(record_b <- other.records) {
      ret.addAndCheckConstraints(mapping.map(record_b(_)))
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

  def where(f: Seq[RelValue[_]]=>RelValue[_]): Relation = {
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
    if(!added.forall(!this.header.contains(_)))
      throw new RuntimeException("Cannot extend an already existing field")

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

  override def toString(): String = {
    val max_height = Array.ofDim[Int](size)
    val max_width = this.header.map(s => s.name.length).toArray
    val strings = Array.ofDim[Seq[ArrayBuffer[String]]](size)

    for((record, i) <- records.zipWithIndex) {
      strings(i) = record.map(i => ArrayBuffer(i.wrapped.toString().split("\n"):_*))
      for((array, j) <- strings(i).zipWithIndex) {
        val max = array.map(_.length()).max
        if(max > max_width(j))
          max_width(j) = max
      }
      val maxh = if(!strings(i).isEmpty) strings(i).map(_.length).max else 1
      if(maxh > max_height(i))
        max_height(i) = maxh
    }

    val header = Array.ofDim[String](arity)
    for(i <- this.header.indices) {
      val s = this.header(i).name
      header(i) = (" "*(max_width(i)-s.length))+s
    }

    /**
      *
      */
    for((row, r) <- strings.zipWithIndex) {
      for((column, c) <- row.zipWithIndex) {
        for ((line, i) <- column.zipWithIndex) {
          column(i) = (" " * (max_width(c)-line.length)) + line
        }
        val space = " " * max_width(c)
        val sup_lines = Array.tabulate(max_height(r)-column.length)(i => space)
        row(c).appendAll(sup_lines)
      }
    }

    val b = new StringBuilder("|")
    header.foreach(h => {
      b.append(h).append("|")
    })

    val len = b.length
    b.insert(0, "="*len+"\n")
    b.append("\n").append("="*len).append("\n")

    for((row, r) <- strings.zipWithIndex) {
        for(i <- 0 until max_height(r)) {
          b.append("|")
          row.foreach(array => {
            b.append(array(i)).append("|")
          })
          b.append("\n")
        }
    }
    b.append("-"*len).append("\n")
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
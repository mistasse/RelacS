package group24.language

import group24.library.{Relation => RRelation, dum, BooleanValue, RelValue}

/**
  * Created by mistasse on 24/04/16.
  */

object RECORD {
  def apply(header: Symbol*)(wrappers: RelValue[_]*) = new PseudoMonadRecord(header, wrappers)
}

class RelationToMonad(val rel: RRelation) {
  def toSet() = new PseudoMonadRelation(rel)
  def foreach[U](f: (PseudoMonadRecord)=>U) = toSet().foreach(f)
  def map[B](f: (PseudoMonadRecord)=>PseudoMonadRecord) = {
    val recordsSet = toSet().map(f)
    var ret: RRelation = if(recordsSet.isEmpty) new RRelation() else null

    for(record <- recordsSet) {
      if(ret == null)
        ret = new RRelation(record.header:_*)
      if(ret.header != record.header)
        throw new RuntimeException("map records with different headers is not allowed")
      ret.addAndCheckConstraints(record.array)
    }

    ret
  }
  def filter(f: (PseudoMonadRecord)=>RelValue[_]): RRelation = {
    val ret = new RRelation(rel.header:_*)

    val recordSet = toSet()
    for(record <- recordSet) {
      if(f(record) equals BooleanValue.TRUE)
        ret.addAndCheckConstraints(record.array)
    }

    ret
  }
  def flatMap(f: (PseudoMonadRecord)=>RRelation) = {
    val relationsSet = toSet().map(f)
    var ret: RRelation = if(relationsSet.isEmpty) new RRelation() else null
    for(relation <- relationsSet) {
      if(ret == null && relation != dum)
        ret = new RRelation(relation.header:_*)
      if(relation.size > 0 && ret.header != relation.header)
        throw new RuntimeException("flatMap with different headers is not allowed")
      for(record <- relation.records)
        ret.addAndCheckConstraints(record)
    }
    if(ret == null)
      dum
    else
      ret
  }
}

class PseudoMonadRecord(val header: Seq[Symbol], val array: Seq[RelValue[_]]) {
  def apply(idx: Symbol): RelValue[_] = array(header.indexOf(idx))
  def apply(idx: Int): RelValue[_] = array(idx)

  override def equals(other: Any): Boolean = {
    if(!other.isInstanceOf[PseudoMonadRecord])
      return false
    val that = other.asInstanceOf[PseudoMonadRecord]
    if(header.toSet != that.header.toSet)
      return false
    val mapping = if(header == that.header) Range(0, header.length) else header.map(that.header.indexOf(_))
    array equals mapping.map(that.array)
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
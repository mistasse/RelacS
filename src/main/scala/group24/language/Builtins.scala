package group24.language

import group24.library.{Relation => RRelation, _}

/**
  * Created by mistasse on 24/04/16.
  */


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

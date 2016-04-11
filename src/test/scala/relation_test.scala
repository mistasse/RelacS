/**
  * Created by mistasse on 8/04/16.
  */
/*
import group24.library.RelValue
import org.scalatest._

class UnitTest extends FlatSpec with RelEnv

class RelationTest extends UnitTest {

  it should "be equal" in {
    assert (new Relation() == dum)
    assert (new Relation().add() == dee)

    val A = new Relation('A, 'B, 'C) add('A->0, 'B->1, 'C->2)
    assert (A == (A project('B, 'A, 'C))) // symmetric transformation
    assert (A == (A project('C, 'A, 'B))) // asymmetric transformation
  }

  "projection" should "make them different" in {
    val A = new Relation('A, 'B, 'C) add('A->0, 'B->1, 'C->2)
    assert (A != (A project('A, 'B)))
    assert (A != (A project('A, 'C)))
    assert (A != (A project('C, 'B)))
  }

  "renaming" should "work and make them different" in {
    val A = new Relation('A, 'B)
    assert (A.rename('A as 'C).header == Seq('C, 'B))
  }

  "duplicates" should "not happen in records" in {
    val A = new Relation('A, 'B)
      .add('A->0, 'B->1)
      .add('A->0, 'B->1)

    assert (A.size == 1)
    A.records += Array[RelValue[_]](0, 1)
    assert (A.size == 1)
  }

  "projection and renaming" should "work together" in {
    val A = new Relation('A, 'B)
      .add('A->0, 'B->1)
      .add('A->1, 'B->2)
      .add('A->2, 'B->3)
    assert ((A rename('A->'C) project('C)) == (A project('A) rename('A->'C)))
  }

  val A = new Relation('A, 'B)
    .add('A->0, 'B->2)
    .add('A->1, 'B->3)
  val B = new Relation('A, 'C)
    .add('A->0, 'C->4)
    .add('A->1, 'C->3)

  "joins" should "be symmetrical" in {
    assert ((A join B) == (B join A))
    assert ((A join dee) == (dee join A))
    assert ((B join dee) == (dee join B))
    assert ((A join dum) == (dum join A))
    assert ((B join dum) == (dum join B))
  }

  "joins" should "be compliant with theory" in {
    assert ((A join A) == A)
    assert ((B join B) == B)
    assert ((A join dum).size == 0)
    assert ((B join dum).size == 0)
    assert ((A join dee) == A)
    assert ((B join dee) == B)
  }
  /*
  "where" should "work correctly and accept external types" in {
    assert ((A where {'A :== 1}) == new Relation('A, 'B).add('A->1, 'B->3))
    assert ((A where {'A :== 0}) == new Relation('A, 'B).add('A->0, 'B->2))
    assert (((A join B) where {'B :== 'C}) == new Relation('A, 'B, 'C).add('A->1, 'B->3, 'C->3))

    val C = new Relation('A).add('A->A)
    assert ((C where {'A :== A}) == C)
  }

  "extend" should "do the basic job" in {
    assert (A.extend('C){'C := ""}.header == Seq('A, 'B, 'C))
    assert ((A.extend('C, 'D){'C := 'A;'D := 'B} project('C, 'D) rename('C as 'A, 'D as 'B)) == A)
    assert ((B.extend('B, 'D){'B := 'A;'D := 'C} project('B, 'D) rename('B as 'A, 'D as 'C)) == B)
    assert (A.extend(){} == A)
    assert ((
      A
        .extend('C, 'D, 'maxA, 'minA) {
          'C := 'A
          'D := 'B
          'maxA := max('A)
          'minA := min('A)
        }
        join new Relation('dupID).add('dupID->0).add('dupID->1)
        where{
          ('C :== 'A) &&
            ('D :== 'B) &&
            ('maxA :== max(A, 'A)) &&
            ('minA :== min(A, 'A)) &&
            ('dupID :== 0)
        }
        project('C, 'D)
        rename('C as 'A, 'D as 'B)
      ) == A)
  }
  */
}
*/
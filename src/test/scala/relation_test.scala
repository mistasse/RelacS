/**
  * Created by mistasse on 8/04/16.
  */
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
}

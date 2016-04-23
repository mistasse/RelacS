/**
  * Created by mistasse on 8/04/16.
  */

import group24.language._
import group24.library._
import org.scalatest._

class UnitTest extends FlatSpec with RelEnv

class DSLTest extends UnitTest {
  it should "be equal" in {
    assert(RELATION() == dum)
    assert((RELATION() &()) == dee)

    val A = RELATION('A, 'B, 'C) &
      (0, 1, 2)

    assert(A == (A PROJECT('B, 'A, 'C)))
    assert(A == (A PROJECT('C, 'A, 'B)))
  }

  "projection" should "make them different" in {
    val A = RELATION('A, 'B, 'C) & (0, 1, 2)
    assert(A != (A project('A, 'B)))
    assert(A != (A project('A, 'C)))
    assert(A != (A project('C, 'B)))
  }

  "renaming" should "work and make them different" in {
    val A = RELATION('A, 'B)
    assert(A.RENAME('A as 'C).header == Seq('C, 'B))
  }

  "union" should "work" in {
    val ABC = RELATION('A, 'B, 'C) & (0, 1, 2)
    val A = RELATION('A, 'B, 'C)
    val B = RELATION('B, 'A, 'C) & (1, 0, 2)
    val C = RELATION('C, 'A, 'B) & (2, 0, 1)

    assert((A UNION B) == ABC)
    assert((A UNION C) == ABC)
    assert((A UNION B).header == ABC.header)
    assert((A UNION C).header == ABC.header)
  }

  "duplicates" should "not happen in records" in {
    val A = RELATION('A, 'B) & (0, 1) & (0, 1)

    assert(A.size == 1)
    A.addAndCheckConstraints(Seq(IntValue(0), IntValue(1)))
    assert(A.size == 1)
  }

  "projection and renaming" should "work together" in {
    val A = RELATION('A, 'B) &(0, 1) &(1, 2) &(2, 3)
    assert((A RENAME ('A as 'C) PROJECT ('C)) == (A PROJECT ('A) RENAME ('A as 'C)))
  }

  val A = RELATION('A, 'B) &(0, 2) &(1, 3)
  val B = RELATION('A, 'C) &(0, 4) &(1, 3)

  "joins" should "be symmetrical" in {
    assert((A join B) == (B join A))
    assert((A join dee) == (dee join A))
    assert((B join dee) == (dee join B))
    assert((A join dum) == (dum join A))
    assert((B join dum) == (dum join B))
  }

  "not_matching" should "work as contrary for join dum/dee" in {
    assert((A not_matching B union(A join B project('A, 'B))) == A)

    assert((A not_matching dum) == (dee join A))
    assert((B not_matching dum) == (dee join B))
    assert((A not_matching dee) == (dum join A))
    assert((B not_matching dee) == (dum join B))
  }

  "joins" should "be compliant with theory" in {
    assert((A join A) == A)
    assert((B join B) == B)
    assert((A join dum).size == 0)
    assert((B join dum).size == 0)
    assert((A join dee) == A)
    assert((B join dee) == B)
  }

  "where" should "work correctly and accept external types" in {
    assert ((A WHERE ('A :== 1)) == (RELATION('A, 'B) & (1, 3)))
    assert ((A WHERE ('A :== 0)) == (RELATION('A, 'B) & (0, 2)))
    assert (((A JOIN B) WHERE ('B :== 'C)) == (RELATION('A, 'B, 'C) & (1, 3, 3)))

    val C = RELATION('A) & (A)
    assert ((C WHERE {'A :== A}) == C)
  }

  "multiple concat" should "work in internal scope" in {
    val C = RELATION('id) &(0) &(1)

    C EXTEND('a := dee JOIN C)

    assert((
      C EXTEND(
        'A := RELATION('reid) &('id) &('id :+ 1) &('id :+ 2)
        )
      WHERE(
        COUNT('A) :== 3
        )
      PROJECT('id)
    ) == C)
  }

  "min and max" should "do their work" in {
    // == doesn't work because we are playing on RelValue => returns a BooleanValue
    assert(MAX(A, 'A) equals IntValue(1))
    assert(MIN(A, 'A) equals IntValue(0))
    assert(MAX(A, 'B) equals IntValue(3))
    assert(MIN(A, 'B) equals IntValue(2))
    assert(COUNT(A) equals IntValue(2))
  }

  "extend" should "do the basic job" in {
    assert ((A EXTEND('C := "")).header == Seq('A, 'B, 'C))
    assert ((A EXTEND('C := 'A,'D := 'B) PROJECT('C, 'D) RENAME('C as 'A, 'D as 'B)) == A)
    assert ((B EXTEND('B := 'A,'D := 'C) PROJECT('B, 'D) RENAME('B as 'A, 'D as 'C)) == B)
    assert ((A EXTEND()) == A)
    assert ((
      A EXTEND (
          'C := 'A,
          'D := 'B,
          'maxA := MAX('A),
          'minA := MIN('A),
          'count := COUNT()
        )
        JOIN RELATION('dupID) &(0) &(1) // Duplicate entries
        WHERE (
          ('C :== 'A) && ('D :== 'B),
          'maxA :== MAX(A, 'A),
          'minA :== MIN(A, 'A),
          'count :== COUNT(A),
          'count :== COUNT() :/ 2,
          COUNT() :/ 2 :== COUNT(A),
          'dupID :<> 1
        )
        PROJECT('C, 'D)
        RENAME('C as 'A, 'D as 'B)
      ) == A)
  }
}
/*

}
*/
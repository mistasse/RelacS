package group24

import group24.language._
import group24.language.DSL._

/**
  * Created by mistasse on 26/04/16.
  */
object Example {

  def main(args: Array[String]) {
    // Join and operator precedence
    val result = RELATION('id, 'name) &
      (1, "Maxime") &
      (0, "Jerome") &
      (2, "Charles") JOIN
    RELATION('id, 'grade) &
      (0, 18) &
      (1, 15) &
      (2, 15) PRINT()

    // Projection
    val students = result PROJECT('id, 'name)
    val grades = result PROJECT('id, 'grade)

    // Rename
    val others = (students JOIN grades
      PROJECT('name, 'grade)
      RENAME('grade as 'otherg))

    // Where, extend
    (
      students JOIN grades
        EXTEND(
        'above := (others where('grade :< 'otherg)
          rename('otherg as 'grade)),
        'below := (others where('grade :> 'otherg)
          rename('otherg as 'grade))
        )
      ) PRINT()

    // JOIN-like monads
    (
      for {
        s <- students
        g <- grades
        if s('id) == g('id)
      } yield RECORD('id, 'name, 'grade)(s('id), s('name), g('grade))
      ) PRINT()

    // UNION-like monads
    val maxjer = RELATION('id, 'name) & (0, "Jerome") & (1, "Maxime")
    val cha = RELATION('id, 'name) & (2, "Charles")
    RELATION('rel) & (maxjer) & (cha) flatMap(r => r('rel).get[Rel]) PRINT()
  }
}

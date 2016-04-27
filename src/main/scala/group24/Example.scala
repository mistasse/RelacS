package group24

import group24.language._
import group24.language.DSL._

/**
  * Created by mistasse on 26/04/16.
  */
object Example {

  def main(args: Array[String]) {

    val students = RELATION('id, 'name) &
      (1, "Maxime") &
      (0, "Jérôme")

    students PRINT()

    val grades = RELATION('id, 'grade) &
      (0, 18) &
      (1, 15)
    students JOIN grades PRINT()

    (
      students JOIN grades
        EXTEND(
        'best := ('grade :== MAX('grade))
        )
        PRINT()
        WHERE('best :== true)
        PRINT()
      )


    val evil = (students JOIN grades
      PROJECT('name, 'grade)
      RENAME('grade as 'otherg))
    (
      students JOIN grades
        EXTEND(
        'above := (evil where('grade :< 'otherg)
          rename('otherg as 'grade)),
        'below := (evil where('grade :> 'otherg)
          rename('otherg as 'grade))
        )
        PRINT()
      )

    val jer = RELATION('id, 'name) & (0, "Jérôme")
    val max = RELATION('id, 'name) & (1, "Maxime")
    (RELATION('rel) &
      (jer) &
      (max)
      PRINT()
      flatMap(r => r('rel).get[Rel])
      ) PRINT()

    /*
        val students = (
          RELATION('id, 'name, 'surname) &
            (0, "Maxime", "Istasse") &
            (1, "Jérôme", "Lemaire") &
            (2, "Léonard", "Julémont")
            PROJECT('id, 'surname, 'name)
          )
        val grades =
          RELATION('id, 'points) &
            (0, 15) &
            (1, 18) &
            (2, 20)

        println(
          students JOIN grades map(r =>
            RECORD('name, 'good)(r('name), r('points) == MAX(grades, 'points))
            )
        )

          (
            RELATION('rel) &
              (RELATION('id, 'name) & (0, "Maxime")) &
              (RELATION('id, 'name) & (0, "Jérôme"))
            PRINT()
            flatMap(r => r('rel).get[Rel])
            PRINT()
          )


        println(for{
          s <- students
          g <- grades
          if g('id) == s('id)
        } yield RECORD('name, 'grade)(s('name), if(g('points) > IntValue(16)) StringValue("GD") else StringValue("D") ))

        println(
          students JOIN grades flatMap(r =>
            if(r('name).wrapped equals "Jérôme")
              None
            else
              RELATION('name, 'good) &
                (r('name), r('points).get[Int] == 20)
            )
        )

        val rel = RELATION('id, 'name) &
          (0, "Maxime") &
          (1, "Jérôme")

        rel PRINT()

        (
          (students JOIN grades)
          PRINT()
          EXTEND (
            'best := ('points :== MAX('points)),
            'better_than := students join grades
              rename('points as 'otherp)
              where('points :> 'otherp)
              project('name, 'surname),
            'lower_than := students join grades
              rename('points as 'otherp)
              where('points :< 'otherp)
              project('surname, 'name)
              union RELATION('name, 'surname) & ("Prof", "The")  // Adding the Prof
            )
          PRINT()
          UNION (
            RELATION('id, 'name, 'surname, 'points) & // Adding the prof
              ("∞", "Prof", "The", "∞")
            EXTEND(
              'best := "more than that",
              'better_than := students join grades project('surname, 'name) join dee, // Better than every student
              'lower_than := students join grades project('surname, 'name) join dum // Nobody is better than him actually
              )
            )
          PRINT()
        )
        */
  }
}

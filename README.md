# RelacS - Relational databases using Scala

#### Disclaimer: This project is an academical project. It has been taken as a challenge to build an expressive DSL from a very poor base library. This basically means performances were not targeted, just expressivity, creativity, and Scala exploration.

## The (very) basic relational engine

A Relation, in implementation terms, is a class formed by a header (an array of symbols) and a body (a set of records). Records are themselves arrays of RelValues, RelValues being wrappers for the values we accept in the relations. They define operators for working with them, and they can be overridden, just like with the Rel software.

No abstraction is put around the Relation class because **the goal was to see how the DSL would be able to fight against that lack of flexibility**. And since the academical statement suggested to take an already existing Java library, it has been assumed it should have been as simple as it could be.

The only types accepted to put into Relations are for now Integers, Booleans, Strings and Relations. It could easily be extended thanks to our *ExtensibleValue* class, see implementations of *StringValue* or any of those to make yourself an idea.

The following operators are accepted by RelValues: `+` `-` `/` `\*` `<` `<=` `>` `>=` `==` `<>` `&&` `||` `join` `union` `not_matching`.

They can also be called on `rename((Symbol, Symbol)*)`, `project(Symbol*)`, `where(Seq[RelValue[_]]=>Boolean)` and `extend(Symbol*)(f: (Array[RelValue[_]])=>Unit)`.

Note how it must be thought before being able to *extend* a relation, or to filter it with *where*. The positions of the attributes you want to modify must be computed by yourself, taking into account every transformation you could have applied on your first representation of the data. Hopefully, the positions are easy to determine. Also, thanks to the DSL, you won't have to ever do that.

It could probably have been possible to use the *Dynamic* feature of Scala in order to make extensions more generic, but it would also have been a loss of time (over-engineering) as well as of Java-likeness. It could be the subject of a future rather easy refactoring.

**It is also important to note no efforts have been put into performances.**

## The DSL

The DSL has been developed in two steps. The first objective was to mimic Tutorial D's syntax and expressiveness as much as possible, as well as its closure behavior. The second step has more been like an exploration around monadic concepts.

### *Tutorial D-like*

The DSL basically allows to create relations in the following fashion:


```scala
val students = RELATION('id, 'name) &
            (1, "Maxime") &
            (0, "Jérôme")

students PRINT()
```

and renders the following:
```
===========
|id|  name|
===========
| 1|Jérôme|
| 0|Maxime|
-----------
```

#### JOIN

From this, it is possible to write the following request, merging those students with their respective grades:

```scala
val grades = RELATION('id, 'grade) &
                (0, 18) &
                (1, 15)
students JOIN grades PRINT()
```

```
=================
|id|  name|grade|
=================
| 0|Jérôme|   18|
| 1|Maxime|   15|
-----------------
```

#### EXTEND, WHERE

Then, imagine you would like to know which one is the best, by adding a field and then filtering your relation while visualizing intermediate steps:
```scala
(
  students JOIN grades
  EXTEND(
    'best := ('grade :== MAX('grade))
    )
  PRINT()
  WHERE('best :== true)
  PRINT()
)
```
with the following output:
```
=======================
|id|  name|grade| best|
=======================
| 0|Jérôme|   18| true|
| 1|Maxime|   15|false|
-----------------------

======================
|id|  name|grade|best|
======================
| 0|Jérôme|   18|true|
----------------------
```

#### NESTED RELATIONS, CLOSURES, RENAMING, PROJECTING, WTF's and lowercases

And now, you would like to associate to every student, the ones that are above and below him in terms of grades. You should prefer using an intermediate relation variable.

```scala
val evil = students JOIN grades RENAME('grade as 'otherg)
(
  students JOIN grades
  EXTEND(
    'above := (evil WHERE('grade :< 'otherg)),
    'below := (evil WHERE('grade :> 'otherg))
    )
  PRINT()
)
```
With the ouput `Exception in thread "main" java.util.NoSuchElementException: key not found: 'grade`... Wait... What?! Not exactly what you expected I guess.

Let us make it simple. The Scala compiler will see that semantically, it should be able to compute `evil WHERE('grade :> 'otherg)` as a static relation, and convert that relation to a closure that returns the it as-is. That closure is then passed into a function that puts its result at the desired offset of the record on evaluation (remember how extends work?). But what it doesn't know, is that it needs a dynamic scope in order to access the attribute `'grade`, since it refers to an attribute in `students JOIN grades`, not in `evil`. (it has been renamed!)

In order to fix this, making the `WHERE` lowercase should work. It will tell the compiler it is not a static one, but one that will be embedded as a closure in the whole extension process as it is handled by the DSL. (it generates several layers of closures) The dynamic resolution works by finding the closest attribute with the specified name in the hierarchy. This is why we had to rename the `'grade` field of `evil`. If we had not, it would shadow the one of the record we are extending.

As a rule of thumb, you can be sure that you'll never be wrong when putting lowercases within `WHERE` or `EXTEND` blocks. When working on a static relation, caps are preferred, or you might end up with something else than the type you expected. We will also discard the `'id` attribute in the embedded relations by projection on the other fields, because the name is enough for us:

```scala
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
```
yielding awesome, yet meaningful ascii art:
```
===============================================
|id|  name|grade|         above|         below|
===============================================
| 0|Jérôme|   18|  ============|==============|
|  |      |     |  |name|grade|||  name|grade||
|  |      |     |  ============|==============|
|  |      |     |  ------------||Maxime|   15||
|  |      |     |              |--------------|
| 1|Maxime|   15|==============|  ============|
|  |      |     ||  name|grade||  |name|grade||
|  |      |     |==============|  ============|
|  |      |     ||Jérôme|   18||  ------------|
|  |      |     |--------------|              |
-----------------------------------------------
```

Then, only your imagination is the limit. Ok, as well as your lifetime with respect to your hardware and the quality of those very trivially implemented algorithms. (Given a certain amount of data, it might become interesting to optimize them)

#### Behind the dynamic scope

It might be interesting for you to know how such thing works. The DSL allows us to go from expressions such as `'attr := ('points :== MAX('points))'` to a closure that modifies arrays, and that even supports nested extensions, ... The DSL maintains an environment within the closures it provides to the basic library. The identifiers, implicitly introduced by the symbols in some dynamic context, will make usage of this environment to return the right value when evaluated.

### Monads

The course the project has been written for also comprises some introduction to monads, and suggested to try to work with them. This is why some things have been implemented in order to make relations kind of monadic.

Here are some sad news. First, our relations are not pure monads. Anything cannot be put into it. Since this is a 2-dimensional collection, with a distinct structure at the first and second levels (a set of records), we really had to distinguish them. So theoretically, our relation can be a monad, but the only type it can accept as contained is Record. Second, we could not make our records monads themselves. It would not be advantageous at all, but we added some syntactic sugar.

There is also another uncertainty. When using for loops onto types that do not implement `map`, `filter`, or any of these. Will the compiler perform implicit conversions that could make it work? Here, the answer is hopefully **yes**.

The idea that has been implemented is the following. Records have to contain their labels, this is what is implemented in *PseudoMonadRecord*. So you can access an attribute using `record('attr)`. Then, we needed a representation of our relations that would be sets of such records. This is the role of *PseudoMonadRelation*. Then, we implemented the monadic methods relying on those of the sets. We couldn't define an implicit conversion from Relation to this representation because of the other operators that would be ambiguous. So we have an intermediate class providing an access to `foreach`, `map`, ... only.

The most basic example we could think of is
```scala
( // equivalent to students map(r => r) PRINT()
  for {
    r <- students
  }
    yield r
) PRINT()
```

#### JOIN-like
In fact, we can imitate a lot of the functions of library using this
```scala
for { // equivalent to students.flatMap(s => grades.filter(g => g('id) == s('id)).map(g => RECORD('id, 'name, 'grade)(s('id), s('name), g('grade))))
  s <- students
  g <- grades
  if s('id) == g('id)
} yield RECORD('id, 'name, 'grade)(s('id), s('name), g('grade))
```

```
=================
|id|  name|grade|
=================
| 0|Jérôme|   18|
| 1|Maxime|   15|
-----------------
```

#### UNION-like
```scala
val jer = RELATION('id, 'name) & (0, "Jérôme")
val max = RELATION('id, 'name) & (1, "Maxime")
(RELATION('rel) &
  (jer) &
  (max)
  flatMap(r => r('rel).get[Rel])
) PRINT()
```
```
===========
|id|  name|
===========
| 1|Maxime|
| 0|Jérôme|
-----------
```

<!--
TODO
-->

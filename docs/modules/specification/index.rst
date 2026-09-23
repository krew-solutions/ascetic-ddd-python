Specification
=============

.. index:: specification

The specification module implements the :term:`Specification` pattern with
multiple query backends.

.. toctree::
   :hidden:

   lambda_filter
   jsonpath_parser


Semantics
---------

A specification has two readers that must agree: ``EvaluateVisitor``, which
decides in memory, and the database reading the query ``PostgresqlVisitor``
compiles the same tree to. So the evaluator's semantics are PostgreSQL's, and
``infrastructure/tests/integration/test_postgresql_agreement.py`` holds the
two against each other on a live database: the same value or the same kind
of failure for each constant expression, the same rows selected for each
specification over rows with nulls, embedded and relational.

* **Nulls.** A comparison or a computation with a null operand is null.
  ``AND``, ``OR``, ``NOT`` are three-valued: ``NULL AND FALSE`` is false,
  ``NULL OR TRUE`` is true, ``NOT NULL`` is null. ``IS``, ``IS NULL``,
  ``IS NOT NULL`` and a wildcard are true or false, never null. A candidate
  satisfies a specification whose value is true; a null does not, as a row
  with a null condition is not selected. ``AND`` and ``OR`` do not evaluate
  their right operand once the left has decided, and a wildcard stops at its
  first witness.
* **Arithmetic** of ``int`` and ``float`` (``specification.domain.arithmetic``):
  integer division truncates towards zero, the remainder has the sign of the
  dividend, a result outside ``bigint`` is an ``OverflowError`` and so is a
  float overflow, the count of a shift is taken modulo 64; ``bool`` and
  ``str`` have no arithmetic. Any other type - a Value Object, ``Decimal``,
  ``datetime`` - computes as it defines.
* **Comparisons** of ``bool``, numbers and ``str``
  (``specification.domain.comparison``): values of different kinds do not
  compare - ``"a" == 1`` and ``True == 1`` are a ``TypeError``, as they are
  "operator does not exist" in the database; ``NaN`` equals itself and is
  greater than any other number; false comes before true. Any other type
  compares as it defines.
* **A null is tested, not compared.** In the tree ``x = NULL`` is null and
  true of nothing. The notations in which null is a value like any other
  mean IS NULL by it, and are parsed so: ``@.a == null`` of a template,
  spelled out or bound to a placeholder, and ``u.a is None`` / ``u.a == None``
  of a lambda become ``IsNull``; ``!=`` and ``is not`` become ``IsNotNull``.
* **Names** that go into a query are letters, digits and ``_``, not starting
  with a digit - of fields, collections, tables, columns and aliases alike;
  anything else is a ``ValueError``, not quoted. Only values are parameters.
* **A collection is named in a schema by its whole path**,
  ``"Categories.Items"``; a relational collection is joined to what its path
  starts at - the enclosing item, or the root row.
* **A template is a function of its parameters**, and a specification has no
  word for a placeholder. ``parse()`` keeps what it parsed as the function
  that builds the tree once the parameters are there; ``bind(params)`` calls
  it and returns the specification, with a ``Value`` where a placeholder
  stood, to evaluate, to transform or to compile to SQL; ``match()`` is
  ``bind()`` and the evaluator. So there is no tree with a placeholder in it
  for a reader to be handed by mistake, and a ``Visitor`` has no method for
  one. A placeholder used to be a ``Value`` whose value was a marker tuple,
  which went into a query as a parameter.
* **A composite is not a node.** ``CompositeExpression`` is what a transform
  context may return instead of a node, ``Mapped``, for a field or a value
  that is several columns; ``=`` and ``!=`` of two composites become nodes,
  part by part. ``transform(context, expression)`` returns a node: a
  composite left over - under any other operator, beside a node, as a
  predicate, as the whole specification - is its ``ValueError``. It used to
  be a ``Visitable`` whose ``accept`` raised from inside whatever visited
  the tree next.
* **A transform context** is an interface to inherit, ``ITransformContext``,
  of two abstract methods: ``attr_node(path)`` for the members,
  ``value_node(val)`` for the values. A mapping is of the aggregate's
  members and knows nothing of any query: it is asked about a member by its
  whole path from the candidate, ``["categories", "products", "price"]``, a
  collection being a member like any other, and answers a path from the
  candidate's row. Where the answer goes - from which item, how far out -
  is the tree's, and the transformer puts it there: a member of an item is
  the answer less the collection's, from the item. It used to ask about the
  members of "the item" by their names alone, ``item_attr_node``, so a
  mapping could not tell the items of one collection from another's, and
  about where a collection is kept, ``collection_node``, which is the
  schema's to say.
* **A schema** is the foreign keys of the storage, as ``\d`` shows them,
  and nothing of any aggregate or query:
  ``foreign_key(table, column, referenced_table, referenced_column)``, a
  composite key by ``foreign_key_composite``, a key named as PostgreSQL
  names it - ``store_items_store_id_fkey`` - or by ``constraint_name``. A
  tree names a collection by its table, and where two keys of that table
  reference the row it is named from, by the key's name; an object kept in
  a table of its own by the key's column, ``owner_id``; a row of an array,
  which has no table, by the array's column, ``"stores.items"``. What the
  schema does not mention is an array or a composite in the row. It used to
  key a collection by its path in the aggregate, then by a name of the
  query's, and carry an alias for the subquery, which is the compiler's to
  make.

Lambda Filter
-------------

See :doc:`lambda_filter` for the Lambda Filter parser documentation.


Native JSONPath
---------------

See :doc:`jsonpath_parser` for the Native JSONPath parser documentation.


One Parser
----------

The template language has one parser, the native one. There were two more,
which converted the tree of a library - jsonpath-rfc9535 and jsonpath2 - and
with them the dependencies on both. They were removed: every change of the
language had to be made three times; they had defects of their own (the path
to a collection lost, placeholders after a nested collection misbound, ``&&``
and ``||`` read left to right); and one defect was of their construction - a
placeholder was a magic value put into the text before the library read it,
so a template with that value for a literal was refused or, worse, answered
wrongly. Their tests of the language pass against the native parser and are
kept: ``test_jsonpath_parser_rfc9535.py``, ``test_jsonpath_parser_edge_cases.py``.

See the :doc:`/api/index` for auto-generated API documentation.

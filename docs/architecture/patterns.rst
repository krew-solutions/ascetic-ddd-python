Design Patterns
===============

.. index:: patterns

This section describes the key design patterns used throughout ascetic-ddd.

Domain Patterns
---------------

.. index:: pair: pattern; Aggregate
.. index:: pair: pattern; Repository
.. index:: pair: pattern; Specification

Aggregate
^^^^^^^^^

The :term:`Aggregate` pattern enforces consistency boundaries. Each aggregate has
a root entity that controls access to its internal objects.

Repository
^^^^^^^^^^

The :term:`Repository` pattern provides collection-like access to aggregates.
Implementations exist for PostgreSQL (``psycopg``-based).

Specification
^^^^^^^^^^^^^

The :term:`Specification` pattern encapsulates query criteria. Supported flavors:

- **JSONPath**: a native parser of RFC 9535 filters with placeholders
- **Lambda filter**: In-memory predicate-based filtering
- **Query lookup**: MongoDB-like query operators (``$eq``, ``$gt``, ``$in``, ``$rel``, etc.)

Infrastructure Patterns
-----------------------

.. index:: pair: pattern; Outbox
.. index:: pair: pattern; Inbox
.. index:: pair: pattern; Saga
.. index:: pair: pattern; Unit of Work

Transactional Outbox
^^^^^^^^^^^^^^^^^^^^

The :term:`Outbox` pattern ensures reliable event publishing by writing events
to a database table within the same transaction as the business operation.

Idempotent Inbox
^^^^^^^^^^^^^^^^

The :term:`Inbox` pattern ensures messages are processed exactly once using
deduplication based on message identifiers.

Saga
^^^^

The :term:`Saga` pattern coordinates distributed transactions across multiple
aggregates or services using compensating actions.

Unit of Work
^^^^^^^^^^^^

Session management implements the Unit of Work pattern, tracking changes and
committing them atomically.

Test Data Patterns
------------------

.. index:: pair: pattern; Provider
.. index:: pair: pattern; Distributor

Provider Topology
^^^^^^^^^^^^^^^^^

The faker module uses a :term:`Provider` topology where each provider is
responsible for generating a specific piece of test data. Providers form
a directed acyclic graph with reference providers linking aggregates.

Distribution Strategies
^^^^^^^^^^^^^^^^^^^^^^^

:term:`Distributor` strategies control how test data is selected or created:

- **Sequence**: Round-robin through existing data
- **Weighted**: Probability-weighted selection
- **Random**: Random selection from available pool

Architecture Overview
=====================

.. index:: architecture; overview

ascetic-ddd is a DDD toolkit and seedwork library organized into two main layers:

Seedwork Layer
--------------

The ``ascetic_ddd.seedwork`` package provides foundational abstractions:

- **Domain primitives**: :term:`Aggregate`, :term:`Entity`, :term:`Value Object`, identity
- **Session management**: Unit of Work pattern with PostgreSQL support
- **Signals**: Typed Signal pattern (``ISyncSignal``/``IAsyncSignal``) for domain events
- **Repository**: Base repository interfaces and PostgreSQL implementation

Application Modules
-------------------

Built on top of the seedwork:

- **Specification**: Query criteria using JSONPath and lambda-based filters
- **Outbox**: Transactional outbox pattern for reliable event publishing
- **Inbox**: Idempotent message processing
- **Saga**: Distributed transaction coordination
- **Event Bus**: In-process event routing
- **Mediator**: Command/query dispatching
- **Faker**: Test data generation with provider topology and distribution strategies

Package Structure
-----------------

.. code-block:: text

   ascetic_ddd/
   +-- seedwork/          # Foundational DDD abstractions
   |   +-- domain/        # Aggregate, Entity, Value Object, Identity
   |   +-- infrastructure/ # PostgreSQL repositories, event store
   +-- specification/     # Specification pattern implementations
   +-- outbox/            # Transactional outbox
   +-- inbox/             # Idempotent inbox
   +-- saga/              # Saga orchestration
   +-- bus/               # In-process message bus
   +-- mediator/          # Command/query mediator
   +-- signals/           # Typed Signal pattern
   +-- session/           # Session/UoW management
   +-- batch/             # Batch processing utilities
   +-- faker/             # Test data generation framework
   +-- utils/             # Shared utilities
   +-- factory.py         # Factory helpers

Design Principles
-----------------

1. **Ascetic**: Minimal abstractions, no unnecessary complexity
2. **Explicit over implicit**: Clear interfaces, no magic
3. **Let it crash**: Natural exceptions over custom wrappers
4. **Portability**: Design with potential Go port in mind
5. **C-style formatting**: ``%s`` / ``%d`` instead of f-strings for cross-language consistency

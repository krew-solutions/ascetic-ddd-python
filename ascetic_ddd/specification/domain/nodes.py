"""AST nodes for Specification pattern."""
from abc import ABC, abstractmethod
from typing import Any, Callable, Protocol

from .constants import OPERATOR, ASSOCIATIVITY


class Visitor(Protocol):
    """Visitor interface for traversing AST nodes."""

    def visit_global_scope(self, node: "GlobalScope") -> None:
        """Visit a global scope node."""
        ...

    def visit_object(self, node: "Object") -> None:
        """Visit an object node."""
        ...

    def visit_collection(self, node: "Collection") -> None:
        """Visit a collection node."""
        ...

    def visit_item(self, node: "Item") -> None:
        """Visit an item node."""
        ...

    def visit_field(self, node: "Field") -> None:
        """Visit a field node."""
        ...

    def visit_value(self, node: "Value") -> None:
        """Visit a value node."""
        ...

    def visit_prefix(self, node: "Prefix") -> None:
        """Visit a prefix node."""
        ...

    def visit_infix(self, node: "Infix") -> None:
        """Visit an infix node."""
        ...

    def visit_postfix(self, node: "Postfix") -> None:
        """Visit a prefix node."""
        ...


class Visitable(ABC):
    """Base class for visitable nodes."""

    @abstractmethod
    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        pass


class Operable(Protocol):
    """Interface for operable nodes."""

    def associativity(self) -> ASSOCIATIVITY:
        """Return operator associativity."""
        ...

    def operator(self) -> OPERATOR:
        """Return operator type."""
        ...


class EmptiableObject(Protocol):
    """Interface for objects that can be empty (scopes)."""

    def parent(self) -> "EmptiableObject":
        """Return parent object."""
        ...

    def name(self) -> str:
        """Return object name."""
        ...

    def is_root(self) -> bool:
        """Check if this is a root object."""
        ...

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        ...


class Value(Visitable):
    """Node representing a constant value."""

    def __init__(self, value: Any):
        self._value = value

    def value(self) -> Any:
        """Return the stored value."""
        return self._value

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_value(self)


class Prefix(Visitable):
    """Node representing a prefix operator (e.g., NOT)."""

    def __init__(
        self,
        operator: OPERATOR,
        operand: Visitable,
        associativity: ASSOCIATIVITY,
    ):
        self._operator = operator
        self._operand = operand
        self._associativity = associativity

    def operand(self) -> Visitable:
        """Return the operand."""
        return self._operand

    def operator(self) -> OPERATOR:
        """Return the operator."""
        return self._operator

    def associativity(self) -> ASSOCIATIVITY:
        """Return operator associativity."""
        return self._associativity

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_prefix(self)


class Not(Prefix):
    def __init__(self, operand: Visitable):
        super().__init__(OPERATOR.NOT, operand, ASSOCIATIVITY.RIGHT_ASSOCIATIVE)


class Infix(Visitable):
    """Node representing an infix operator (e.g., AND, OR, =, >)."""

    def __init__(
        self,
        left: Visitable,
        operator: OPERATOR,
        right: Visitable,
        associativity: ASSOCIATIVITY,
    ):
        self._left = left
        self._operator = operator
        self._right = right
        self._associativity = associativity

    def left(self) -> Visitable:
        """Return the left operand."""
        return self._left

    def operator(self) -> OPERATOR:
        """Return the operator."""
        return self._operator

    def right(self) -> Visitable:
        """Return the right operand."""
        return self._right

    def associativity(self) -> ASSOCIATIVITY:
        """Return operator associativity."""
        return self._associativity

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_infix(self)


class Equal(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.EQ, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class NotEqual(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.NE, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class Is(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.IS, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class GreaterThan(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.GT, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class LessThan(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.LT, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class GreaterThanEqual(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.GTE, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class LessThanEqual(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.LTE, right, ASSOCIATIVITY.NON_ASSOCIATIVE)


class And(Infix):
    def __init__(self, left: Visitable, *rights: Visitable):
        if not rights:
            raise ValueError("At least one right operand is required")
        left_folded, right_folded = _fold_rights(type(self), left, *rights)
        super().__init__(left_folded, OPERATOR.AND, right_folded, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Or(Infix):
    def __init__(self, left: Visitable, *rights: Visitable):
        if not rights:
            raise ValueError("At least one right operand is required")
        left_folded, right_folded = _fold_rights(type(self), left, *rights)
        super().__init__(left_folded, OPERATOR.OR, right_folded, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class LeftShift(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.LSHIFT, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class RightShift(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.RSHIFT, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Add(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.ADD, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Sub(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.SUB, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Mul(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.MUL, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Div(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.DIV, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


class Mod(Infix):
    def __init__(self, left: Visitable, right: Visitable):
        super().__init__(left, OPERATOR.MOD, right, ASSOCIATIVITY.LEFT_ASSOCIATIVE)


def _fold_rights(
    callable_func: Callable[..., Infix],
    left: Visitable,
    *rights: Visitable,
) -> tuple[Visitable, Visitable]:
    """Fold multiple right operands into nested binary operations."""
    rights_list = list(rights)
    while len(rights_list) > 1:
        left = callable_func(left, rights_list.pop(0))
    return left, rights_list[0]


class Postfix(Visitable):
    """Node representing a prefix operator (e.g., NOT)."""

    def __init__(
        self,
        operand: Visitable,
        operator: OPERATOR,
        associativity: ASSOCIATIVITY,
    ):
        self._operand = operand
        self._operator = operator
        self._associativity = associativity

    def operand(self) -> Visitable:
        """Return the operand."""
        return self._operand

    def operator(self) -> OPERATOR:
        """Return the operator."""
        return self._operator

    def associativity(self) -> ASSOCIATIVITY:
        """Return operator associativity."""
        return self._associativity

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_postfix(self)


class IsNull(Postfix):
    def __init__(self, operand: Visitable):
        super().__init__(operand, OPERATOR.IS_NULL, ASSOCIATIVITY.NON_ASSOCIATIVE)


class IsNotNull(Postfix):
    def __init__(self, operand: Visitable):
        super().__init__(operand, OPERATOR.IS_NOT_NULL, ASSOCIATIVITY.NON_ASSOCIATIVE)


class GlobalScope(Visitable):
    """Node representing the global scope (root)."""

    def parent(self) -> "GlobalScope":
        """Return parent (self for root)."""
        return self

    def name(self) -> str:
        """Return the name."""
        return "Empty"

    def is_root(self) -> bool:
        """Check if this is root."""
        return True

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_global_scope(self)


class Object(Visitable):
    """Node representing an object in the scope chain."""

    def __init__(self, parent: EmptiableObject, name: str):
        self._parent = parent
        self._name = name

    def parent(self) -> EmptiableObject:
        """Return the parent object."""
        return self._parent

    def name(self) -> str:
        """Return the object name."""
        return self._name

    def is_root(self) -> bool:
        """Check if this is root."""
        return False

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_object(self)


class Collection(Visitable):
    """Node representing a collection with a wildcard and predicate."""

    def __init__(self, parent: EmptiableObject, name: str, predicate: Visitable):
        self._parent = parent
        self._name = name
        self._predicate = predicate

    def parent(self) -> EmptiableObject:
        """Return the parent object."""
        return self._parent

    def name(self) -> str:
        """Return the collection name (*)."""
        return self._name

    def is_root(self) -> bool:
        """Check if this is root."""
        return False

    def predicate(self) -> Visitable:
        """Return the predicate for filtering collection items."""
        return self._predicate

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_collection(self)


class Wildcard(Collection):
    def __init__(self, parent: EmptiableObject, predicate: Visitable):
        super().__init__(parent, "*", predicate)


class Item(Visitable):
    """Node representing the current item in a collection (@)."""

    def parent(self) -> EmptiableObject:
        """Return parent (global scope)."""
        return GlobalScope()

    def name(self) -> str:
        """Return the item name (@)."""
        return "@"

    def is_root(self) -> bool:
        """Check if this is root."""
        return True

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_item(self)


class Field(Visitable):
    """Node representing a field access."""

    def __init__(self, obj: EmptiableObject, name: str):
        self._object = obj
        self._name = name

    def name(self) -> str:
        """Return the field name."""
        return self._name

    def object(self) -> EmptiableObject:
        """Return the object containing this field."""
        return self._object

    def accept(self, visitor: Visitor) -> None:
        """Accept a visitor."""
        visitor.visit_field(self)


def extract_field_path(node: Field) -> list[str]:
    """Extract the full path to a field as a list of names."""
    path = [node.name()]
    obj: EmptiableObject = node.object()
    while not obj.is_root():
        path.insert(0, obj.name())
        obj = obj.parent()
    return path

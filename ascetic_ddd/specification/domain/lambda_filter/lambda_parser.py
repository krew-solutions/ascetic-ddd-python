"""
Lambda function parser for Specification Pattern.

Parses Python lambda functions and converts them to Specification AST nodes.
Inspired by hypothesis.internal.filtering and hypothesis.internal.lambda_sources.
"""
import ast
import collections
import dataclasses
import inspect
from typing import Any, Callable, Mapping

from ascetic_ddd.specification.domain.nodes import (
    Add,
    And,
    Div,
    EmptiableObject,
    Equal,
    Field,
    GlobalScope,
    GreaterThan,
    GreaterThanEqual,
    IsNotNull,
    IsNull,
    Item,
    LessThan,
    LessThanEqual,
    Mod,
    Mul,
    Neg,
    Not,
    NotEqual,
    Or,
    Sub,
    Value,
    Visitable,
    Wildcard,
    Object,
    equality_or_null_test,
)


@dataclasses.dataclass(frozen=True)
class _Scope:
    """What the names of the lambda stand for at a point of its body.

    Entering a comprehension makes a new scope rather than changing this one,
    so what a name meant outside is what it means again after it.
    """
    # The lambda's argument: the candidate. A path from it is from
    # GlobalScope(), inside a comprehension as well as outside.
    root: str
    # The target of the nearest enclosing comprehension: a path from it is
    # from Item().
    item: str | None = None
    # The targets of the comprehensions further out. The tree has one Item(),
    # the nearest, so these can be named in Python and not in the tree.
    outer: tuple[str, ...] = ()

    def inside(self, item: str) -> "_Scope":
        """The scope of the body of a comprehension whose target is ``item``."""
        outer = self.outer if self.item is None else self.outer + (self.item,)
        return _Scope(root=self.root, item=item, outer=outer)

    def owner(self, name: str) -> GlobalScope | Item | None:
        """The node a path from ``name`` starts at; None for a name from outside.

        Raises:
            ValueError: If ``name`` is the item of an outer comprehension.
        """
        if name == self.item:
            return Item()
        if name == self.root:
            return GlobalScope()
        if name in self.outer:
            raise ValueError(
                "The item of an outer comprehension cannot be referred to"
                " from an inner one: %s" % name
            )
        return None


def _free_variables(predicate: Callable[[Any], bool]) -> Mapping[str, Any]:
    """The variables a lambda can see from where it was written.

    Those of the enclosing functions shadow those of the module, as they do
    for the lambda itself.
    """
    cells = predicate.__closure__ or ()
    nonlocals = {
        name: cell.cell_contents
        for name, cell in zip(predicate.__code__.co_freevars, cells)
    }
    return collections.ChainMap(nonlocals, predicate.__globals__)


class LambdaParser:
    """Parser for lambda functions to Specification AST."""

    def __init__(self, predicate: Callable[[Any], bool]):
        """
        Initialize parser with a lambda function.

        Args:
            predicate: Lambda function to parse (e.g., lambda x: x.age > 25)
        """
        self.predicate = predicate
        # What a name that is not of the lambda's own stands for: its value at
        # the time of parsing becomes a Value node, so a specification can
        # have parameters - ``lambda u: u.age > min_age``.
        self._free_variables = _free_variables(predicate)

    def parse(self) -> Visitable:
        """
        Parse lambda function to Specification AST.

        Returns:
            Visitable AST node

        Raises:
            ValueError: If lambda cannot be parsed

        Examples:
            >>> parse(lambda user: user.age > 25)
            GreaterThan(Field(GlobalScope(), "age"), Value(25))

            >>> parse(lambda user: user.age > 25 and user.active == True)
            And(GreaterThan(...), Equal(...))

            >>> parse(lambda store: any(item.price > 500 for item in store.items))
            Wildcard(Object(GlobalScope(), "items"), GreaterThan(Field(Item(), "price"), Value(500)))
        """
        # Get source and parse - need to find lambda in the source
        try:
            source_lines, lineno = inspect.findsource(self.predicate)
            source = "".join(source_lines)
            tree = ast.parse(source)
        except Exception as e:
            raise ValueError(f"Cannot parse lambda source: {e}") from e

        # Find all lambda nodes in the AST
        lambdas = self._find_all_lambdas(tree)

        # Find the lambda that matches our predicate by line number
        lambda_node = None
        target_lineno = self.predicate.__code__.co_firstlineno

        # Find closest lambda by line number
        closest = None
        closest_dist = float('inf')

        for candidate in lambdas:
            if len(candidate.args.args) == 1:
                dist = abs(candidate.lineno - target_lineno)
                if dist < closest_dist:
                    closest_dist = dist
                    closest = candidate

        lambda_node = closest

        if lambda_node is None:
            raise ValueError("Cannot find lambda in source")

        if len(lambda_node.args.args) != 1:
            raise ValueError("Lambda must have exactly one argument")

        return self._convert_node(lambda_node.body, _Scope(root=lambda_node.args.args[0].arg))

    def _find_all_lambdas(self, tree: ast.AST) -> list[ast.Lambda]:
        """Find all lambda nodes in AST."""
        lambdas = []

        class Visitor(ast.NodeVisitor):
            def visit_Lambda(self, node):
                lambdas.append(node)
                self.visit(node.body)

        Visitor().visit(tree)
        return lambdas

    def _convert_node(self, node: ast.AST, scope: _Scope) -> Visitable:
        """Convert AST node to Specification node."""
        # Comparison operators
        if isinstance(node, ast.Compare):
            return self._convert_compare(node, scope)

        # Boolean operations (and, or)
        if isinstance(node, ast.BoolOp):
            return self._convert_bool_op(node, scope)

        # Unary operations (not, -, +)
        if isinstance(node, ast.UnaryOp):
            return self._convert_unary_op(node, scope)

        # Binary operations (+, -, *, /, %)
        if isinstance(node, ast.BinOp):
            return self._convert_bin_op(node, scope)

        # Function calls (any, all, etc.)
        if isinstance(node, ast.Call):
            return self._convert_call(node, scope)

        # Attribute access (e.g., user.age), or a value from outside the
        # lambda reached by its attributes (e.g., limits.max_price)
        if isinstance(node, ast.Attribute):
            free = self._free_value(node, scope)
            if free is not None:
                return free
            return self._convert_attribute(node, scope)

        # Constants
        if isinstance(node, ast.Constant):
            return Value(node.value)

        # Names (variables)
        if isinstance(node, ast.Name):
            owner = scope.owner(node.id)
            if owner is not None:
                return owner
            # It's a constant from enclosing scope
            free = self._free_value(node, scope)
            assert free is not None
            return free

        raise ValueError(f"Unsupported AST node: {type(node).__name__}")

    def _free_value(self, node: ast.AST, scope: _Scope) -> Value | None:
        """
        The value of a name from outside the lambda, or of its attributes.

        Examples:
            min_age -> Value(25)
            limits.max_price -> Value(500)
            user.age -> None (a path from the lambda's own argument)

        Raises:
            ValueError: If the name is not the lambda's and is nowhere else.
        """
        if isinstance(node, ast.Name):
            if scope.owner(node.id) is not None:
                return None
            if node.id not in self._free_variables:
                raise ValueError(f"Non-local variable: {node.id}")
            return Value(self._free_variables[node.id])
        if isinstance(node, ast.Attribute):
            owner = self._free_value(node.value, scope)
            if owner is None:
                return None
            return Value(getattr(owner.value(), node.attr))
        return None

    def _convert_compare(self, node: ast.Compare, scope: _Scope) -> Visitable:
        """Convert comparison node to Specification node."""
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise ValueError("Only simple comparisons are supported")

        left = self._convert_node(node.left, scope)
        right = self._convert_node(node.comparators[0], scope)
        op = node.ops[0]

        # A comparison with None - spelled out, or a variable from outside the
        # lambda that is None - is the null test: in the tree a comparison
        # with null is null, as in SQL, and would be true of nothing.
        if isinstance(op, (ast.Eq, ast.Is)):
            return equality_or_null_test(Equal, left, right)
        elif isinstance(op, (ast.NotEq, ast.IsNot)):
            return equality_or_null_test(NotEqual, left, right)
        elif isinstance(op, ast.Gt):
            return GreaterThan(left, right)
        elif isinstance(op, ast.Lt):
            return LessThan(left, right)
        elif isinstance(op, ast.GtE):
            return GreaterThanEqual(left, right)
        elif isinstance(op, ast.LtE):
            return LessThanEqual(left, right)
        else:
            raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")

    def _convert_bool_op(self, node: ast.BoolOp, scope: _Scope) -> Visitable:
        """Convert boolean operation (and, or) to Specification node."""
        if len(node.values) < 2:
            raise ValueError("Boolean operation must have at least 2 operands")

        values = [self._convert_node(v, scope) for v in node.values]

        if isinstance(node.op, ast.And):
            result = values[0]
            for val in values[1:]:
                result = And(result, val)
            return result
        elif isinstance(node.op, ast.Or):
            result = values[0]
            for val in values[1:]:
                result = Or(result, val)
            return result
        else:
            raise ValueError(f"Unsupported boolean operator: {type(node.op).__name__}")

    def _convert_unary_op(self, node: ast.UnaryOp, scope: _Scope) -> Visitable:
        """Convert unary operation (not, -, +) to Specification node."""
        operand = self._convert_node(node.operand, scope)

        if isinstance(node.op, ast.Not):
            return Not(operand)
        elif isinstance(node.op, ast.USub):
            # ``-5`` is the constant it looks like, not a negation of ``5``.
            if isinstance(node.operand, ast.Constant) and isinstance(operand, Value):
                return Value(-operand.value())
            return Neg(operand)
        elif isinstance(node.op, ast.UAdd):
            # The tree has no unary plus: ``+x`` is ``x`` in both its readers.
            return operand
        else:
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    def _convert_bin_op(self, node: ast.BinOp, scope: _Scope) -> Visitable:
        """Convert binary operation (+, -, *, /, %) to Specification node."""
        left = self._convert_node(node.left, scope)
        right = self._convert_node(node.right, scope)

        if isinstance(node.op, ast.Add):
            return Add(left, right)
        elif isinstance(node.op, ast.Sub):
            return Sub(left, right)
        elif isinstance(node.op, ast.Mult):
            return Mul(left, right)
        elif isinstance(node.op, ast.Div):
            return Div(left, right)
        elif isinstance(node.op, ast.Mod):
            return Mod(left, right)
        else:
            raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")

    # Mapping of method names to comparison node classes.
    # Value Object comparison methods: receiver.Method(arg) -> NodeClass(receiver, arg)
    _METHOD_COMPARISON_MAP = {
        "Equal": Equal,
        "Equals": Equal,
        "Eq": Equal,
        "NotEqual": NotEqual,
        "NotEquals": NotEqual,
        "Ne": NotEqual,
        "Neq": NotEqual,
        "LessThan": LessThan,
        "Lt": LessThan,
        "LessThanOrEqual": LessThanEqual,
        "LessThanEqual": LessThanEqual,
        "Lte": LessThanEqual,
        "Le": LessThanEqual,
        "GreaterThan": GreaterThan,
        "Gt": GreaterThan,
        "GreaterThanOrEqual": GreaterThanEqual,
        "GreaterThanEqual": GreaterThanEqual,
        "Gte": GreaterThanEqual,
        "Ge": GreaterThanEqual,
    }

    # Mapping of method names to postfix node classes.
    # Postfix methods: receiver.Method() -> NodeClass(receiver)
    _METHOD_POSTFIX_MAP = {
        "IsNull": IsNull,
        "IsNotNull": IsNotNull,
    }

    def _convert_call(self, node: ast.Call, scope: _Scope) -> Visitable:
        """
        Convert function call to Specification node.

        Supports:
        - any([generator expression]) -> Wildcard
        - any([list comprehension]) -> Wildcard
        - all([generator expression]) -> Not(Wildcard(Not(...)))
        - receiver.Eq(arg) -> Equal(receiver, arg)
        - receiver.IsNull() -> IsNull(receiver)
        - etc.
        """
        if isinstance(node.func, ast.Name):
            if node.func.id == "any":
                return self._convert_any(node, scope)
            elif node.func.id == "all":
                return self._convert_all(node, scope)

        # Method calls on attributes (e.g., user.age.Eq(25), user.email.IsNull())
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr

            # Value Object comparison methods
            node_class = self._METHOD_COMPARISON_MAP.get(method_name)
            if node_class is not None:
                return self._convert_method_comparison(node, node_class, scope)

            # Postfix methods
            postfix_class = self._METHOD_POSTFIX_MAP.get(method_name)
            if postfix_class is not None:
                return self._convert_method_postfix(node, postfix_class, scope)

        raise ValueError("Unsupported function call: %s" % ast.unparse(node))

    def _convert_method_comparison(self, node: ast.Call, node_class: type, scope: _Scope) -> Visitable:
        """
        Convert method-based comparison to Specification node.

        Example:
            user.age.Eq(25) -> Equal(Field(GlobalScope(), "age"), Value(25))
            user.profile.age.Gt(18) -> GreaterThan(Field(Object(GlobalScope(), "profile"), "age"), Value(18))
        """
        assert isinstance(node.func, ast.Attribute)
        if len(node.args) != 1:
            raise ValueError("%s() requires exactly 1 argument" % node.func.attr)

        # receiver becomes left operand
        left = self._convert_node(node.func.value, scope)
        # method argument becomes right operand
        right = self._convert_node(node.args[0], scope)

        return node_class(left, right)

    def _convert_method_postfix(self, node: ast.Call, node_class: type, scope: _Scope) -> Visitable:
        """
        Convert postfix method call to Specification node.

        Example:
            user.email.IsNull() -> IsNull(Field(GlobalScope(), "email"))
        """
        assert isinstance(node.func, ast.Attribute)
        if len(node.args) != 0:
            raise ValueError("%s() takes no arguments" % node.func.attr)

        # receiver becomes operand
        operand = self._convert_node(node.func.value, scope)

        return node_class(operand)

    def _convert_any(self, node: ast.Call, scope: _Scope) -> Visitable:
        """
        Convert any() call to Wildcard node.

        Examples:
            any(item.price > 500 for item in store.items)
            -> Wildcard(Object(GlobalScope(), "items"), GreaterThan(Field(Item(), "price"), Value(500)))
        """
        if len(node.args) != 1:
            raise ValueError("any() must have exactly one argument")

        arg = node.args[0]

        # Generator expression: any(item.price > 500 for item in store.items)
        if isinstance(arg, ast.GeneratorExp):
            return self._convert_generator_to_wildcard(arg, scope)

        # List comprehension: any([item.price > 500 for item in store.items])
        if isinstance(arg, ast.ListComp):
            return self._convert_listcomp_to_wildcard(arg, scope)

        raise ValueError(f"Unsupported any() argument: {type(arg).__name__}")

    def _convert_all(self, node: ast.Call, scope: _Scope) -> Visitable:
        """
        Convert all() call to Not(Wildcard(Not(...))).

        all() means every item must satisfy the condition, which is "not any
        item violates the condition", and is written so: the tree needs no
        second quantifier, and no visitor a second case.

        It used to be converted to the same Wildcard as any(), by which
        "every item is active" was read as "some item is active".

        Examples:
            all(item.active for item in store.items)
            -> Not(Wildcard(Object(GlobalScope(), "items"), Not(Field(Item(), "active"))))
        """
        if len(node.args) != 1:
            raise ValueError("all() must have exactly one argument")

        arg = node.args[0]

        if isinstance(arg, (ast.GeneratorExp, ast.ListComp)):
            collection_object, predicate = self._convert_comprehension(arg, scope)
            return Not(Wildcard(collection_object, Not(predicate)))

        raise ValueError(f"Unsupported all() argument: {type(arg).__name__}")

    def _convert_generator_to_wildcard(self, node: ast.GeneratorExp, scope: _Scope) -> Wildcard:
        """
        Convert generator expression to Wildcard.

        Example:
            any(item.price > 500 for item in store.items)
            generator: [comprehension(target=Name('item'), iter=Attribute(Name('store'), 'items'))]
        """
        collection_object, predicate = self._convert_comprehension(node, scope)
        return Wildcard(collection_object, predicate)

    def _convert_listcomp_to_wildcard(self, node: ast.ListComp, scope: _Scope) -> Wildcard:
        """
        Convert list comprehension to Wildcard.

        Example:
            any([item.price > 500 for item in store.items])
        """
        collection_object, predicate = self._convert_comprehension(node, scope)
        return Wildcard(collection_object, predicate)

    def _convert_comprehension(
        self, node: ast.GeneratorExp | ast.ListComp, scope: _Scope
    ) -> tuple[Object, Visitable]:
        """
        Convert a comprehension to the collection it iterates and the predicate on its items.

        Example:
            item.price > 500 for item in store.items
            -> (Object(GlobalScope(), "items"), GreaterThan(Field(Item(), "price"), Value(500)))
        """
        if len(node.generators) != 1:
            raise ValueError("Only single generator is supported")

        gen = node.generators[0]

        # Extract collection path: store.items -> ["items"]
        collection_parent, collection_name = self._extract_collection_path(gen.iter, scope)

        # Parse predicate with new context (item variable)
        if not isinstance(gen.target, ast.Name):
            raise ValueError("Only simple target names are supported in comprehensions")

        # Convert the predicate body: inside it the target is the item
        predicate = self._convert_node(node.elt, scope.inside(gen.target.id))

        # Create Wildcard node with proper parent
        collection_object = Object(collection_parent, collection_name)
        return collection_object, predicate

    def _extract_collection_path(self, node: ast.AST, scope: _Scope) -> tuple[EmptiableObject, str]:
        """
        Extract collection path from iterator.

        Examples:
            store.items -> (Object(GlobalScope(), "store"), "items")
            items -> (GlobalScope(), "items")
        """
        if isinstance(node, ast.Attribute):
            # store.items
            parent = self._get_parent_from_value(node.value, scope)
            return (parent, node.attr)
        elif isinstance(node, ast.Name):
            # items (direct collection)
            if scope.owner(node.id) is not None:
                raise ValueError("Cannot iterate over lambda argument itself")
            # Assume it's a field of the root object
            return (GlobalScope(), node.id)
        else:
            raise ValueError(f"Unsupported collection iterator: {type(node).__name__}")

    def _get_parent_from_value(self, node: ast.AST, scope: _Scope) -> EmptiableObject:
        """Get parent object from value node."""
        if isinstance(node, ast.Name):
            # In item context (nested wildcard), use Item() as parent
            # Otherwise use GlobalScope()
            # e.g., category.items where category is from outer comprehension
            owner = scope.owner(node.id)
            if owner is not None:
                return owner
            else:
                # Some other variable - not supported in our simple case
                raise ValueError(f"Non-local variable: {node.id}")
        elif isinstance(node, ast.Attribute):
            # Nested access: a.b.c.items
            grandparent = self._get_parent_from_value(node.value, scope)
            return Object(grandparent, node.attr)
        else:
            raise ValueError(f"Unsupported parent value: {type(node).__name__}")

    def _convert_attribute(self, node: ast.Attribute, scope: _Scope) -> Field:
        """
        Convert attribute access to Field node.

        Examples:
            user.age -> Field(GlobalScope(), "age")
            item.price -> Field(Item(), "price") (when in collection context)
        """
        # Get the object (parent)
        if isinstance(node.value, ast.Name):
            # In item context (inside wildcard), use Item()
            # Otherwise use GlobalScope()
            owner = scope.owner(node.value.id)
            if owner is None:
                raise ValueError(f"Non-local variable: {node.value.id}")
            obj: EmptiableObject = owner
        elif isinstance(node.value, ast.Attribute):
            # Nested attribute: user.profile.name
            parent_field = self._convert_attribute(node.value, scope)
            # Convert Field to Object for chaining
            obj = Object(parent_field.object(), parent_field.name())
        else:
            raise ValueError(f"Unsupported attribute value: {type(node.value).__name__}")

        return Field(obj, node.attr)


def parse(predicate: Callable[[Any], bool]) -> Visitable:
    """
    Parse lambda function to Specification AST.

    Args:
        predicate: Lambda function to parse

    Returns:
        Visitable AST node representing the specification

    Examples:
        >>> # Simple comparison
        >>> spec = parse(lambda user: user.age > 25)
        >>> # Result: GreaterThan(Field(GlobalScope(), "age"), Value(25))

        >>> # Logical operators
        >>> spec = parse(lambda user: user.age > 25 and user.active == True)
        >>> # Result: And(GreaterThan(...), Equal(...))

        >>> # Wildcard (any)
        >>> spec = parse(lambda store: any(item.price > 500 for item in store.items))
        >>> # Result: Wildcard(Object(GlobalScope(), "items"), GreaterThan(Field(Item(), "price"), Value(500)))

        >>> # List comprehension
        >>> spec = parse(lambda store: any([item.price > 500 for item in store.items]))
        >>> # Result: Wildcard(...)

        >>> # NOT operator
        >>> spec = parse(lambda user: not user.deleted)
        >>> # Result: Not(Field(GlobalScope(), "deleted"))
    """
    parser = LambdaParser(predicate)
    return parser.parse()

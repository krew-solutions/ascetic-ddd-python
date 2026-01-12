"""
Lambda function parser for Specification Pattern.

Parses Python lambda functions and converts them to Specification AST nodes.
Inspired by hypothesis.internal.filtering and hypothesis.internal.lambda_sources.
"""
import ast
import inspect
from typing import Any, Callable

from ..nodes import (
    Add,
    And,
    Div,
    Equal,
    Field,
    GlobalScope,
    GreaterThan,
    GreaterThanEqual,
    Item,
    LessThan,
    LessThanEqual,
    Mod,
    Mul,
    Not,
    NotEqual,
    Or,
    Sub,
    Value,
    Visitable,
    Wildcard,
    Object,
)


class LambdaParser:
    """Parser for lambda functions to Specification AST."""

    def __init__(self, predicate: Callable[[Any], bool]):
        """
        Initialize parser with a lambda function.

        Args:
            predicate: Lambda function to parse (e.g., lambda x: x.age > 25)
        """
        self.predicate = predicate
        self.arg_name: str | None = None
        self._in_item_context: bool = False  # Track if we're in wildcard context

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

        self.arg_name = lambda_node.args.args[0].arg
        return self._convert_node(lambda_node.body)

    def _find_all_lambdas(self, tree: ast.AST) -> list[ast.Lambda]:
        """Find all lambda nodes in AST."""
        lambdas = []

        class Visitor(ast.NodeVisitor):
            def visit_Lambda(self, node):
                lambdas.append(node)
                self.visit(node.body)

        Visitor().visit(tree)
        return lambdas

    def _convert_node(self, node: ast.AST) -> Visitable:
        """Convert AST node to Specification node."""
        # Comparison operators
        if isinstance(node, ast.Compare):
            return self._convert_compare(node)

        # Boolean operations (and, or)
        if isinstance(node, ast.BoolOp):
            return self._convert_bool_op(node)

        # Unary operations (not)
        if isinstance(node, ast.UnaryOp):
            return self._convert_unary_op(node)

        # Binary operations (+, -, *, /, %)
        if isinstance(node, ast.BinOp):
            return self._convert_bin_op(node)

        # Function calls (any, all, etc.)
        if isinstance(node, ast.Call):
            return self._convert_call(node)

        # Attribute access (e.g., user.age)
        if isinstance(node, ast.Attribute):
            return self._convert_attribute(node)

        # Constants
        if isinstance(node, ast.Constant):
            return Value(node.value)

        # Names (variables)
        if isinstance(node, ast.Name):
            if node.id == self.arg_name:
                return GlobalScope()
            # It's a constant from enclosing scope
            raise ValueError(f"Non-local variable: {node.id}")

        raise ValueError(f"Unsupported AST node: {type(node).__name__}")

    def _convert_compare(self, node: ast.Compare) -> Visitable:
        """Convert comparison node to Specification node."""
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise ValueError("Only simple comparisons are supported")

        left = self._convert_node(node.left)
        right = self._convert_node(node.comparators[0])
        op = node.ops[0]

        if isinstance(op, ast.Eq):
            return Equal(left, right)
        elif isinstance(op, ast.NotEq):
            return NotEqual(left, right)
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

    def _convert_bool_op(self, node: ast.BoolOp) -> Visitable:
        """Convert boolean operation (and, or) to Specification node."""
        if len(node.values) < 2:
            raise ValueError("Boolean operation must have at least 2 operands")

        values = [self._convert_node(v) for v in node.values]

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

    def _convert_unary_op(self, node: ast.UnaryOp) -> Visitable:
        """Convert unary operation (not) to Specification node."""
        operand = self._convert_node(node.operand)

        if isinstance(node.op, ast.Not):
            return Not(operand)
        else:
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    def _convert_bin_op(self, node: ast.BinOp) -> Visitable:
        """Convert binary operation (+, -, *, /, %) to Specification node."""
        left = self._convert_node(node.left)
        right = self._convert_node(node.right)

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

    def _convert_call(self, node: ast.Call) -> Visitable:
        """
        Convert function call to Specification node.

        Supports:
        - any([generator expression]) -> Wildcard
        - any([list comprehension]) -> Wildcard
        """
        if isinstance(node.func, ast.Name):
            if node.func.id == "any":
                return self._convert_any(node)
            elif node.func.id == "all":
                return self._convert_all(node)

        raise ValueError(f"Unsupported function call: {ast.unparse(node)}")

    def _convert_any(self, node: ast.Call) -> Visitable:
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
            return self._convert_generator_to_wildcard(arg)

        # List comprehension: any([item.price > 500 for item in store.items])
        if isinstance(arg, ast.ListComp):
            return self._convert_listcomp_to_wildcard(arg)

        raise ValueError(f"Unsupported any() argument: {type(arg).__name__}")

    def _convert_all(self, node: ast.Call) -> Visitable:
        """
        Convert all() call to Wildcard node.

        all() means every item must satisfy the condition, which is equivalent to
        "not any item violates the condition", but for simplicity we treat it
        as Wildcard (same semantic in our context).
        """
        if len(node.args) != 1:
            raise ValueError("all() must have exactly one argument")

        arg = node.args[0]

        if isinstance(arg, ast.GeneratorExp):
            return self._convert_generator_to_wildcard(arg)

        if isinstance(arg, ast.ListComp):
            return self._convert_listcomp_to_wildcard(arg)

        raise ValueError(f"Unsupported all() argument: {type(arg).__name__}")

    def _convert_generator_to_wildcard(self, node: ast.GeneratorExp) -> Wildcard:
        """
        Convert generator expression to Wildcard.

        Example:
            any(item.price > 500 for item in store.items)
            generator: [comprehension(target=Name('item'), iter=Attribute(Name('store'), 'items'))]
        """
        if len(node.generators) != 1:
            raise ValueError("Only single generator is supported")

        gen = node.generators[0]

        # Extract collection path: store.items -> ["items"]
        collection_parent, collection_name = self._extract_collection_path(gen.iter)

        # Save original context
        original_arg_name = self.arg_name
        original_in_item_context = self._in_item_context

        # Parse predicate with new context (item variable)
        if isinstance(gen.target, ast.Name):
            self.arg_name = gen.target.arg if hasattr(gen.target, 'arg') else gen.target.id
        else:
            raise ValueError("Only simple target names are supported in comprehensions")

        # Set item context for wildcard
        self._in_item_context = True

        # Convert the predicate body
        predicate = self._convert_node(node.elt)

        # Restore original context
        self.arg_name = original_arg_name
        self._in_item_context = original_in_item_context

        # Create Wildcard node with proper parent
        collection_object = Object(collection_parent, collection_name)
        return Wildcard(collection_object, predicate)

    def _convert_listcomp_to_wildcard(self, node: ast.ListComp) -> Wildcard:
        """
        Convert list comprehension to Wildcard.

        Example:
            any([item.price > 500 for item in store.items])
        """
        if len(node.generators) != 1:
            raise ValueError("Only single generator is supported")

        gen = node.generators[0]

        # Extract collection path: store.items -> ["items"]
        collection_parent, collection_name = self._extract_collection_path(gen.iter)

        # Save original context
        original_arg_name = self.arg_name
        original_in_item_context = self._in_item_context

        # Parse predicate with new context (item variable)
        if isinstance(gen.target, ast.Name):
            self.arg_name = gen.target.arg if hasattr(gen.target, 'arg') else gen.target.id
        else:
            raise ValueError("Only simple target names are supported in comprehensions")

        # Set item context for wildcard
        self._in_item_context = True

        # Convert the predicate body
        predicate = self._convert_node(node.elt)

        # Restore original context
        self.arg_name = original_arg_name
        self._in_item_context = original_in_item_context

        # Create Wildcard node with proper parent
        collection_object = Object(collection_parent, collection_name)
        return Wildcard(collection_object, predicate)

    def _extract_collection_path(self, node: ast.AST) -> tuple[Object, str]:
        """
        Extract collection path from iterator.

        Examples:
            store.items -> (Object(GlobalScope(), "store"), "items")
            items -> (GlobalScope(), "items")
        """
        if isinstance(node, ast.Attribute):
            # store.items
            parent = self._get_parent_from_value(node.value)
            return (parent, node.attr)
        elif isinstance(node, ast.Name):
            # items (direct collection)
            if node.id == self.arg_name:
                raise ValueError("Cannot iterate over lambda argument itself")
            # Assume it's a field of the root object
            return (GlobalScope(), node.id)
        else:
            raise ValueError(f"Unsupported collection iterator: {type(node).__name__}")

    def _get_parent_from_value(self, node: ast.AST) -> Object:
        """Get parent object from value node."""
        if isinstance(node, ast.Name):
            if node.id == self.arg_name:
                # In item context (nested wildcard), use Item() as parent
                # Otherwise use GlobalScope()
                # e.g., category.items where category is from outer comprehension
                return Item() if self._in_item_context else GlobalScope()
            else:
                # Some other variable - not supported in our simple case
                raise ValueError(f"Non-local variable: {node.id}")
        elif isinstance(node, ast.Attribute):
            # Nested access: a.b.c.items
            grandparent = self._get_parent_from_value(node.value)
            return Object(grandparent, node.attr)
        else:
            raise ValueError(f"Unsupported parent value: {type(node).__name__}")

    def _convert_attribute(self, node: ast.Attribute) -> Field:
        """
        Convert attribute access to Field node.

        Examples:
            user.age -> Field(GlobalScope(), "age")
            item.price -> Field(Item(), "price") (when in collection context)
        """
        # Get the object (parent)
        if isinstance(node.value, ast.Name):
            if node.value.id == self.arg_name:
                # In item context (inside wildcard), use Item()
                # Otherwise use GlobalScope()
                obj = Item() if self._in_item_context else GlobalScope()
            else:
                raise ValueError(f"Non-local variable: {node.value.id}")
        elif isinstance(node.value, ast.Attribute):
            # Nested attribute: user.profile.name
            parent_field = self._convert_attribute(node.value)
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

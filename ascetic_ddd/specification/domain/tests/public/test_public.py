"""Unit tests for Specification public API."""
import unittest
from datetime import datetime

from ... import nodes
from ...public.adapters import Delegating, Logical, Nullable, Comparison, Mathematical, object_, field
from ...public.datatypes import Boolean, NullBoolean, Number, NullNumber, Datetime, NullDatetime, Text, NullText


class TestDelegating(unittest.TestCase):
    """Test Delegating adapter."""

    def test_creation(self):
        """Test creating a Delegating instance."""
        value_node = nodes.Value(42)
        delegating = Delegating(value_node)
        self.assertEqual(delegating.delegate(), value_node)

    def test_delegate_returns_visitable(self):
        """Test that delegate returns a Visitable node."""
        value_node = nodes.Value("test")
        delegating = Delegating(value_node)
        delegate = delegating.delegate()
        self.assertIsInstance(delegate, nodes.Visitable)


class TestLogical(unittest.TestCase):
    """Test Logical adapter."""

    def test_and_operation(self):
        """Test AND operation creates nodes.And."""
        left = Logical(nodes.Value(True))
        right = Logical(nodes.Value(False))
        result = left & right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.And)

    def test_or_operation(self):
        """Test OR operation creates nodes.Or."""
        left = Logical(nodes.Value(True))
        right = Logical(nodes.Value(False))
        result = left | right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Or)

    def test_is_operation(self):
        """Test is_ operation creates nodes.Is."""
        left = Logical(nodes.Value(True))
        right = Logical(nodes.Value(True))
        result = left.is_(right)

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Is)


class TestNullable(unittest.TestCase):
    """Test Nullable adapter."""

    def test_is_null(self):
        """Test is_null creates nodes.IsNull."""
        nullable = Nullable(nodes.Value(None))
        result = nullable.is_null()

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.IsNull)

    def test_is_not_null(self):
        """Test is_not_null creates nodes.IsNotNull."""
        nullable = Nullable(nodes.Value(42))
        result = nullable.is_not_null()

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.IsNotNull)


class TestComparison(unittest.TestCase):
    """Test Comparison adapter."""

    def test_equal(self):
        """Test equality comparison."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(5))
        result = left == right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Equal)

    def test_not_equal(self):
        """Test inequality comparison."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(10))
        result = left != right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.NotEqual)

    def test_greater_than(self):
        """Test greater-than comparison."""
        left = Comparison(nodes.Value(10))
        right = Comparison(nodes.Value(5))
        result = left > right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.GreaterThan)

    def test_less_than(self):
        """Test less-than comparison."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(10))
        result = left < right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.LessThan)

    def test_greater_than_equal(self):
        """Test greater-than-or-equal comparison."""
        left = Comparison(nodes.Value(10))
        right = Comparison(nodes.Value(10))
        result = left >= right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.GreaterThanEqual)

    def test_less_than_equal(self):
        """Test less-than-or-equal comparison."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(5))
        result = left <= right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.LessThanEqual)

    def test_lshift(self):
        """Test left shift operation."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(1))
        result = left << right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.LeftShift)

    def test_rshift(self):
        """Test right shift operation."""
        left = Comparison(nodes.Value(5))
        right = Comparison(nodes.Value(1))
        result = left >> right

        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.RightShift)


class TestMathematical(unittest.TestCase):
    """Test Mathematical adapter."""

    def test_add(self):
        """Test addition operation."""
        left = Mathematical[int](nodes.Value(5))
        right = Mathematical[int](nodes.Value(3))
        result = left + right

        self.assertIsInstance(result, Mathematical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Add)

    def test_sub(self):
        """Test subtraction operation."""
        left = Mathematical[int](nodes.Value(5))
        right = Mathematical[int](nodes.Value(3))
        result = left - right

        self.assertIsInstance(result, Mathematical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Sub)

    def test_mul(self):
        """Test multiplication operation."""
        left = Mathematical[int](nodes.Value(5))
        right = Mathematical[int](nodes.Value(3))
        result = left * right

        self.assertIsInstance(result, Mathematical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Mul)

    def test_div(self):
        """Test division operation."""
        left = Mathematical[int](nodes.Value(6))
        right = Mathematical[int](nodes.Value(3))
        result = left.__div__(right)

        self.assertIsInstance(result, Mathematical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Div)

    def test_mod(self):
        """Test modulo operation."""
        left = Mathematical[int](nodes.Value(5))
        right = Mathematical[int](nodes.Value(3))
        result = left % right

        self.assertIsInstance(result, Mathematical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.Mod)


class TestDatatypes(unittest.TestCase):
    """Test datatype classes."""

    def test_boolean_inheritance(self):
        """Test Boolean inherits from Logical."""
        boolean = Boolean(nodes.Value(True))
        self.assertIsInstance(boolean, Logical)

    def test_null_boolean_inheritance(self):
        """Test NullBoolean inherits from Boolean and Nullable."""
        null_boolean = NullBoolean(nodes.Value(None))
        self.assertIsInstance(null_boolean, Boolean)
        self.assertIsInstance(null_boolean, Nullable)

    def test_number_inheritance(self):
        """Test Number inherits from Comparison and Mathematical."""
        number = Number[int](nodes.Value(42))
        self.assertIsInstance(number, Comparison)
        self.assertIsInstance(number, Mathematical)

    def test_null_number_inheritance(self):
        """Test NullNumber inherits from Number and Nullable."""
        null_number = NullNumber[int](nodes.Value(None))
        self.assertIsInstance(null_number, Number)
        self.assertIsInstance(null_number, Nullable)

    def test_datetime_inheritance(self):
        """Test Datetime inherits from Comparison and Mathematical."""
        dt = Datetime(nodes.Value(datetime.now()))
        self.assertIsInstance(dt, Comparison)
        self.assertIsInstance(dt, Mathematical)

    def test_null_datetime_inheritance(self):
        """Test NullDatetime inherits from Datetime and Nullable."""
        null_dt = NullDatetime(nodes.Value(None))
        self.assertIsInstance(null_dt, Datetime)
        self.assertIsInstance(null_dt, Nullable)

    def test_text_inheritance(self):
        """Test Text inherits from Comparison."""
        text = Text[str](nodes.Value("hello"))
        self.assertIsInstance(text, Comparison)

    def test_null_text_inheritance(self):
        """Test NullText inherits from Text and Nullable."""
        null_text = NullText[str](nodes.Value(None))
        self.assertIsInstance(null_text, Text)
        self.assertIsInstance(null_text, Nullable)


class TestFieldFactory(unittest.TestCase):
    """Test Factory.make_field() method."""

    def test_boolean_field_creation(self):
        """Test creating a Boolean field."""
        bf = Boolean.make_field("is_active")
        self.assertIsInstance(bf, Boolean)
        delegate = bf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_null_boolean_field_creation(self):
        """Test creating a NullBoolean field."""
        nbf = NullBoolean.make_field("is_deleted")
        self.assertIsInstance(nbf, NullBoolean)
        delegate = nbf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_number_field_creation(self):
        """Test creating a Number field."""
        nf = Number[int].make_field("age")
        self.assertIsInstance(nf, Number)
        delegate = nf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_null_number_field_creation(self):
        """Test creating a NullNumber field."""
        nnf = NullNumber[int].make_field("score")
        self.assertIsInstance(nnf, NullNumber)
        delegate = nnf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_datetime_field_creation(self):
        """Test creating a Datetime field."""
        df = Datetime.make_field("created_at")
        self.assertIsInstance(df, Datetime)
        delegate = df.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_null_datetime_field_creation(self):
        """Test creating a NullDatetime field."""
        ndf = NullDatetime.make_field("deleted_at")
        self.assertIsInstance(ndf, NullDatetime)
        delegate = ndf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_text_field_creation(self):
        """Test creating a Text field."""
        tf = Text[str].make_field("name")
        self.assertIsInstance(tf, Text)
        delegate = tf.delegate()
        self.assertIsInstance(delegate, nodes.Field)

    def test_null_text_field_creation(self):
        """Test creating a NullText field."""
        ntf = NullText[str].make_field("description")
        self.assertIsInstance(ntf, NullText)
        delegate = ntf.delegate()
        self.assertIsInstance(delegate, nodes.Field)


class TestValueFactory(unittest.TestCase):
    """Test Factory.make_value() method."""

    def test_boolean_value_creation(self):
        """Test creating a Boolean value."""
        bv = Boolean.make_value(True)
        self.assertIsInstance(bv, Boolean)
        delegate = bv.delegate()
        self.assertIsInstance(delegate, nodes.Value)

    def test_number_value_creation(self):
        """Test creating a Number value."""
        nv = Number[int].make_value(42)
        self.assertIsInstance(nv, Number)
        delegate = nv.delegate()
        self.assertIsInstance(delegate, nodes.Value)

    def test_datetime_value_creation(self):
        """Test creating a Datetime value."""
        now = datetime.now()
        dv = Datetime.make_value(now)
        self.assertIsInstance(dv, Datetime)
        delegate = dv.delegate()
        self.assertIsInstance(delegate, nodes.Value)

    def test_text_value_creation(self):
        """Test creating a Text value."""
        tv = Text[str].make_value("hello")
        self.assertIsInstance(tv, Text)
        delegate = tv.delegate()
        self.assertIsInstance(delegate, nodes.Value)


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions."""

    def test_object_simple_name(self):
        """Test object_ with simple name."""
        obj = object_("user")
        self.assertIsInstance(obj, nodes.Object)
        self.assertEqual(obj.name(), "user")

    def test_object_dotted_name(self):
        """Test object_ with dotted name."""
        obj = object_("user.profile")
        self.assertIsInstance(obj, nodes.Object)
        self.assertEqual(obj.name(), "profile")
        parent = obj.parent()
        self.assertIsInstance(parent, nodes.Object)
        self.assertEqual(parent.name(), "user")

    def test_object_multiple_dots(self):
        """Test object_ with multiple dots."""
        obj = object_("root.user.profile")
        self.assertIsInstance(obj, nodes.Object)
        self.assertEqual(obj.name(), "profile")

    def test_field_simple_name(self):
        """Test field with simple name."""
        f = field("name")
        self.assertIsInstance(f, nodes.Field)
        self.assertEqual(f.name(), "name")
        parent = f.object()
        self.assertIsInstance(parent, nodes.GlobalScope)

    def test_field_dotted_name(self):
        """Test field with dotted name."""
        f = field("user.name")
        self.assertIsInstance(f, nodes.Field)
        self.assertEqual(f.name(), "name")
        parent = f.object()
        self.assertIsInstance(parent, nodes.Object)
        self.assertEqual(parent.name(), "user")

    def test_field_multiple_dots(self):
        """Test field with multiple dots in object path."""
        f = field("root.user.name")
        self.assertIsInstance(f, nodes.Field)
        self.assertEqual(f.name(), "name")
        parent = f.object()
        self.assertIsInstance(parent, nodes.Object)
        self.assertEqual(parent.name(), "user")


class TestIntegration(unittest.TestCase):
    """Integration tests for public API."""

    def test_field_comparison(self):
        """Test comparing two fields."""
        age = Number[int].make_field("age")
        min_age = Number[int].make_value(18)

        result = age > min_age
        self.assertIsInstance(result, Logical)
        delegate = result.delegate()
        self.assertIsInstance(delegate, nodes.GreaterThan)

    def test_field_logical_operations(self):
        """Test logical operations on field comparisons."""
        age = Number[int].make_field("age")
        is_active = Boolean.make_field("is_active")

        age_check = age >= Number[int].make_value(18)
        combined = age_check & is_active

        self.assertIsInstance(combined, Logical)
        delegate = combined.delegate()
        self.assertIsInstance(delegate, nodes.And)

    def test_nullable_field_operations(self):
        """Test nullable field operations."""
        email = NullText[str].make_field("email")

        is_null_check = email.is_null()
        is_not_null_check = email.is_not_null()

        self.assertIsInstance(is_null_check, Logical)
        self.assertIsInstance(is_not_null_check, Logical)

    def test_complex_expression(self):
        """Test building a complex expression."""
        age = Number[int].make_field("age")
        name = Text[str].make_field("name")
        is_active = Boolean.make_field("is_active")

        # (age > 18) AND (name == "Alice") AND is_active
        age_check = age > Number[int].make_value(18)
        name_check = name == Text[str].make_value("Alice")
        expression = (age_check & name_check) & is_active

        self.assertIsInstance(expression, Logical)

    def test_mathematical_operations(self):
        """Test mathematical operations on number fields."""
        price = Number[int].make_field("price")
        quantity = Number[int].make_field("quantity")
        discount = Number[int].make_field("discount")

        # (price * quantity) - discount
        total = (price * quantity) - discount

        # Should return Number type (base type)
        self.assertIsInstance(total, Number)
        self.assertIsInstance(total, Mathematical)
        delegate = total.delegate()
        self.assertIsInstance(delegate, nodes.Sub)

    def test_shift_operations(self):
        """Test shift operations on comparison fields."""
        value = Number[int].make_field("value")
        shift_amount = Number[int].make_value(2)

        left_shifted = value << shift_amount
        right_shifted = value >> shift_amount

        self.assertIsInstance(left_shifted, Logical)
        self.assertIsInstance(right_shifted, Logical)

    def test_modulo_operation(self):
        """Test modulo operation."""
        number = Number[int].make_field("number")
        divisor = Number[int].make_value(10)

        remainder = number % divisor

        # Should return Number type (base type)
        self.assertIsInstance(remainder, Number)
        self.assertIsInstance(remainder, Mathematical)
        delegate = remainder.delegate()
        self.assertIsInstance(delegate, nodes.Mod)


if __name__ == "__main__":
    unittest.main()

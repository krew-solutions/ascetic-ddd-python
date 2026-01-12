import re
import unittest

from ..validators import (
    Validator,
    Required,
    Regex,
    Email,
    Length,
    Number,
    ChainValidator,
    MultivalueValidator,
    MappingValidator,
)
from ...validators.exceptions import (
    ValidationError,
    ChainValidationError,
    MappingValidationError,
)


class TestValidator(unittest.IsolatedAsyncioTestCase):
    """Тесты для базового класса Validator"""

    async def test_default_message(self):
        validator = Validator()
        self.assertEqual(validator.msg, 'Improper value "%s"')

    async def test_custom_message(self):
        custom_msg = "Custom error message"
        validator = Validator(msg=custom_msg)
        self.assertEqual(validator.msg, custom_msg)


class TestRequired(unittest.IsolatedAsyncioTestCase):
    """Тесты для Required валидатора"""

    async def test_raises_on_none(self):
        validator = Required()
        with self.assertRaises(ValidationError) as exc_info:
            await validator(None)
        self.assertEqual(str(exc_info.exception.args[0]), "The value is required")

    async def test_raises_on_empty_string(self):
        validator = Required()
        with self.assertRaises(ValidationError):
            await validator('')

    async def test_raises_on_empty_list(self):
        validator = Required()
        with self.assertRaises(ValidationError):
            await validator([])

    async def test_passes_on_non_empty_string(self):
        validator = Required()
        await validator("some value")

    async def test_passes_on_non_empty_list(self):
        validator = Required()
        await validator([1, 2, 3])

    async def test_passes_on_zero(self):
        validator = Required()
        await validator(0)

    async def test_passes_on_false(self):
        validator = Required()
        await validator(False)

    async def test_custom_message(self):
        custom_msg = "Field is mandatory"
        validator = Required(msg=custom_msg)
        with self.assertRaises(ValidationError) as exc_info:
            await validator(None)
        self.assertEqual(str(exc_info.exception.args[0]), custom_msg)

    async def test_all_empty_values(self):
        validator = Required()
        for value in [None, '', []]:
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    await validator(value)

    async def test_all_non_empty_values(self):
        validator = Required()
        for value in ["text", 123, 0, False, [1], {"key": "value"}]:
            with self.subTest(value=value):
                await validator(value)


class TestRegex(unittest.IsolatedAsyncioTestCase):
    """Тесты для Regex валидатора"""

    async def test_passes_on_match(self):
        validator = Regex(regex=re.compile(r'^\d+$'))
        await validator("12345")

    async def test_raises_on_no_match(self):
        validator = Regex(regex=re.compile(r'^\d+$'))
        with self.assertRaises(ValidationError) as exc_info:
            await validator("abc")
        self.assertIn("abc", str(exc_info.exception.args[1]))

    async def test_custom_message(self):
        custom_msg = "Must be digits only"
        validator = Regex(regex=re.compile(r'^\d+$'), msg=custom_msg)
        with self.assertRaises(ValidationError) as exc_info:
            await validator("abc")
        self.assertEqual(str(exc_info.exception.args[0]), custom_msg)

    async def test_alphanumeric_pattern(self):
        validator = Regex(regex=re.compile(r'^[a-zA-Z0-9]+$'))
        await validator("Test123")
        with self.assertRaises(ValidationError):
            await validator("Test 123")

    async def test_phone_pattern(self):
        validator = Regex(regex=re.compile(r'^\+?\d{10,15}$'))
        await validator("+1234567890")
        await validator("1234567890")
        with self.assertRaises(ValidationError):
            await validator("123")


class TestEmail(unittest.IsolatedAsyncioTestCase):
    """Тесты для Email валидатора"""

    async def test_valid_email(self):
        validator = Email()
        await validator("test@example.com")

    async def test_valid_email_with_subdomain(self):
        validator = Email()
        await validator("user@mail.example.com")

    async def test_valid_email_with_plus(self):
        validator = Email()
        await validator("user+tag@example.com")

    async def test_valid_email_with_dots(self):
        validator = Email()
        await validator("first.last@example.com")

    async def test_invalid_email_no_at(self):
        validator = Email()
        with self.assertRaises(ValidationError):
            await validator("userexample.com")

    async def test_invalid_email_no_domain(self):
        validator = Email()
        with self.assertRaises(ValidationError):
            await validator("user@")

    async def test_invalid_email_no_local_part(self):
        validator = Email()
        with self.assertRaises(ValidationError):
            await validator("@example.com")

    async def test_invalid_email_invalid_tld(self):
        validator = Email()
        with self.assertRaises(ValidationError):
            await validator("user@example.invalid")

    async def test_valid_emails(self):
        validator = Email()
        for email in [
            "test@example.com",
            "user@mail.gov",
            "admin@site.org",
            "contact@company.net",
            "info@service.biz",
        ]:
            with self.subTest(email=email):
                await validator(email)

    async def test_invalid_emails(self):
        validator = Email()
        for email in [
            "invalid",
            "@example.com",
            "user@",
            "user@.com",
            "user @example.com",
        ]:
            with self.subTest(email=email):
                with self.assertRaises(ValidationError):
                    await validator(email)


class TestLength(unittest.IsolatedAsyncioTestCase):
    """Тесты для Length валидатора"""

    async def test_valid_length_within_range(self):
        validator = Length(min_length=1, max_length=10)
        await validator("hello")

    async def test_raises_on_too_short(self):
        validator = Length(min_length=5, max_length=10)
        with self.assertRaises(ValidationError) as exc_info:
            await validator("hi")
        self.assertIn("Wrong length", str(exc_info.exception.args[0]))

    async def test_raises_on_too_long(self):
        validator = Length(min_length=1, max_length=5)
        with self.assertRaises(ValidationError):
            await validator("verylongtext")

    async def test_no_max_length(self):
        validator = Length(min_length=3)
        await validator("short")
        await validator("very long text that should pass")

    async def test_boundary_min_length(self):
        validator = Length(min_length=5, max_length=10)
        with self.assertRaises(ValidationError):
            await validator("1234")

    async def test_boundary_max_length(self):
        validator = Length(min_length=1, max_length=10)
        with self.assertRaises(ValidationError):
            await validator("12345678901")

    async def test_custom_message(self):
        custom_msg = "Invalid text length"
        validator = Length(min_length=1, max_length=10, msg=custom_msg)
        with self.assertRaises(ValidationError) as exc_info:
            await validator("")
        self.assertEqual(str(exc_info.exception.args[0]), custom_msg)

    async def test_assertion_error_on_invalid_range(self):
        with self.assertRaises(AssertionError):
            Length(min_length=10, max_length=5)

    async def test_converts_to_string(self):
        validator = Length(min_length=1, max_length=10)
        await validator(12345)

    async def test_various_lengths(self):
        test_cases = [
            ("abc", 1, 5, True),
            ("a", 2, 5, False),
            ("abcdef", 1, 5, False),
            ("ab", 1, None, True),
            ("", 1, 10, False),
        ]
        for value, min_len, max_len, should_pass in test_cases:
            with self.subTest(value=value, min_len=min_len, max_len=max_len):
                validator = Length(min_length=min_len, max_length=max_len)
                if should_pass:
                    await validator(value)
                else:
                    with self.assertRaises(ValidationError):
                        await validator(value)


class TestNumber(unittest.IsolatedAsyncioTestCase):
    """Тесты для Number валидатора"""

    async def test_valid_number_within_range(self):
        validator = Number(minimum=0, maximum=100)
        await validator(50)

    async def test_raises_on_too_small(self):
        validator = Number(minimum=10, maximum=100)
        with self.assertRaises(ValidationError):
            await validator(5)

    async def test_raises_on_too_large(self):
        validator = Number(minimum=0, maximum=100)
        with self.assertRaises(ValidationError):
            await validator(150)

    async def test_raises_on_non_numeric(self):
        validator = Number(minimum=0, maximum=100)
        with self.assertRaises(ValidationError):
            await validator("not a number")

    async def test_no_minimum(self):
        validator = Number(maximum=100)
        await validator(-1000)
        await validator(50)

    async def test_no_maximum(self):
        validator = Number(minimum=0)
        await validator(0)
        await validator(1000000)

    async def test_float_values(self):
        validator = Number(minimum=0.0, maximum=1.0)
        await validator(0.5)

    async def test_boundary_minimum(self):
        validator = Number(minimum=10, maximum=20)
        await validator(10)

    async def test_boundary_maximum(self):
        validator = Number(minimum=10, maximum=20)
        await validator(20)

    async def test_custom_message(self):
        custom_msg = "Number out of range"
        validator = Number(minimum=0, maximum=100, msg=custom_msg)
        with self.assertRaises(ValidationError) as exc_info:
            await validator(200)
        self.assertEqual(exc_info.exception.args[0], custom_msg)

    async def test_assertion_error_on_invalid_range(self):
        with self.assertRaises(AssertionError):
            Number(minimum=100, maximum=10)

    async def test_various_numbers(self):
        test_cases = [
            (50, 0, 100, True),
            (0, 0, 100, True),
            (100, 0, 100, True),
            (50, None, 100, True),
            (50, 0, None, True),
            (-10, 0, 100, False),
        ]
        for value, minimum, maximum, should_pass in test_cases:
            with self.subTest(value=value, minimum=minimum, maximum=maximum):
                validator = Number(minimum=minimum, maximum=maximum)
                if should_pass:
                    await validator(value)
                else:
                    with self.assertRaises(ValidationError):
                        await validator(value)


class TestChainValidator(unittest.IsolatedAsyncioTestCase):
    """Тесты для ChainValidator"""

    async def test_all_validators_pass(self):
        validator = ChainValidator(
            Required(),
            Length(min_length=3, max_length=10)
        )
        await validator("hello")

    async def test_first_validator_fails(self):
        validator = ChainValidator(
            Required(),
            Length(min_length=3, max_length=10)
        )
        with self.assertRaises(ChainValidationError):
            await validator(None)

    async def test_second_validator_fails(self):
        validator = ChainValidator(
            Required(),
            Length(min_length=3, max_length=10)
        )
        with self.assertRaises(ChainValidationError):
            await validator("hi")

    async def test_multiple_validators_fail(self):
        validator = ChainValidator(
            Length(min_length=5, max_length=10),
            Regex(regex=re.compile(r'^\d+$'))
        )
        with self.assertRaises(ChainValidationError) as exc_info:
            await validator("ab")
        errors = exc_info.exception.args[0]
        self.assertEqual(len(errors), 2)

    async def test_complex_chain(self):
        validator = ChainValidator(
            Required(),
            Length(min_length=3, max_length=50),
            Email()
        )
        await validator("test@example.com")
        with self.assertRaises(ChainValidationError):
            await validator("ab")

    async def test_raises_on_non_callable(self):
        with self.assertRaises(AssertionError):
            validator = ChainValidator(Required(), "not a validator")
            await validator("test")

    async def test_empty_validators_list(self):
        validator = ChainValidator()
        await validator("anything")

    async def test_single_validator(self):
        validator = ChainValidator(Required())
        await validator("test")
        with self.assertRaises(ChainValidationError):
            await validator(None)


class TestMultivalueValidator(unittest.IsolatedAsyncioTestCase):
    """Тесты для MultivalueValidator"""

    async def test_all_values_pass(self):
        """Тест: все значения проходят валидацию"""
        validator = MultivalueValidator(Required())
        await validator(["value1", "value2", "value3"])

    async def test_single_value_fails(self):
        """Тест: одно значение не проходит валидацию"""
        validator = MultivalueValidator(Required())
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["value1", None, "value3"])
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)
        self.assertNotIn(0, errors)
        self.assertNotIn(2, errors)

    async def test_multiple_values_fail(self):
        """Тест: несколько значений не проходят валидацию"""
        validator = MultivalueValidator(Required())
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator([None, "value2", "", "value4"])
        errors = exc_info.exception.args[0]
        self.assertIn(0, errors)
        self.assertIn(2, errors)
        self.assertNotIn(1, errors)
        self.assertNotIn(3, errors)

    async def test_all_values_fail(self):
        """Тест: все значения не проходят валидацию"""
        validator = MultivalueValidator(Required())
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator([None, "", []])
        errors = exc_info.exception.args[0]
        self.assertEqual(len(errors), 3)
        self.assertIn(0, errors)
        self.assertIn(1, errors)
        self.assertIn(2, errors)

    async def test_empty_collection(self):
        """Тест: пустая коллекция проходит валидацию"""
        validator = MultivalueValidator(Required())
        await validator([])

    async def test_with_length_validator(self):
        """Тест: валидация длины строк"""
        validator = MultivalueValidator(Length(min_length=3, max_length=10))
        await validator(["abc", "hello", "world"])

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["ab", "hello", "verylongtext"])
        errors = exc_info.exception.args[0]
        self.assertIn(0, errors)
        self.assertIn(2, errors)
        self.assertNotIn(1, errors)

    async def test_with_number_validator(self):
        """Тест: валидация чисел"""
        validator = MultivalueValidator(Number(minimum=0, maximum=100))
        await validator([10, 50, 100])

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator([10, -5, 150, 50])
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)
        self.assertIn(2, errors)
        self.assertNotIn(0, errors)
        self.assertNotIn(3, errors)

    async def test_with_email_validator(self):
        """Тест: валидация email адресов"""
        validator = MultivalueValidator(Email())
        await validator(["test@example.com", "user@mail.org"])

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["valid@example.com", "invalid", "also@valid.com"])
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)
        self.assertNotIn(0, errors)
        self.assertNotIn(2, errors)

    async def test_with_regex_validator(self):
        """Тест: валидация с регулярным выражением"""
        validator = MultivalueValidator(Regex(regex=re.compile(r'^\d+$')))
        await validator(["123", "456", "789"])

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["123", "abc", "456", "xyz"])
        errors = exc_info.exception.args[0]
        self.assertEqual(len(errors), 2)
        self.assertIn(1, errors)
        self.assertIn(3, errors)

    async def test_with_chain_validator(self):
        """Тест: валидация с цепочкой валидаторов"""
        validator = MultivalueValidator(
            ChainValidator(
                Required(),
                Length(min_length=3, max_length=50),
                Email()
            )
        )
        await validator(["test@example.com", "user@mail.org"])

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["valid@example.com", "ab", "invalid"])
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)
        self.assertIn(2, errors)
        self.assertNotIn(0, errors)

    async def test_tuple_collection(self):
        """Тест: работа с tuple"""
        validator = MultivalueValidator(Required())
        await validator(("value1", "value2", "value3"))

        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(("value1", None))
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)

    async def test_set_collection(self):
        """Тест: работа с set"""
        validator = MultivalueValidator(Number(minimum=0, maximum=100))
        await validator({10, 50, 90})

    async def test_error_indices_match_positions(self):
        """Тест: индексы ошибок соответствуют позициям в коллекции"""
        validator = MultivalueValidator(Length(min_length=3, max_length=10))
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["okk", "ab", "fine", "x", "good"])
        errors = exc_info.exception.args[0]
        self.assertIn(1, errors)  # "ab" at index 1
        self.assertIn(3, errors)  # "x" at index 3
        self.assertEqual(len(errors), 2)

    async def test_preserves_original_error_messages(self):
        """Тест: сохраняются оригинальные сообщения об ошибках"""
        custom_msg = "Custom validation error"
        validator = MultivalueValidator(Required(msg=custom_msg))
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator(["value", None])
        errors = exc_info.exception.args[0]
        self.assertEqual(str(errors[1].args[0]), custom_msg)

    async def test_various_collections(self):
        """Тест: различные типы коллекций и сценарии"""
        test_cases = [
            (["test@example.com", "user@mail.org"], Email(), True),
            (["test@example.com", "invalid"], Email(), False),
            ([10, 20, 30], Number(minimum=0, maximum=100), True),
            ([10, -5, 30], Number(minimum=0, maximum=100), False),
            (["abc", "hello"], Length(min_length=3, max_length=10), True),
            (["ab", "hello"], Length(min_length=3, max_length=10), False),
            ([], Required(), True),
        ]

        for values, inner_validator, should_pass in test_cases:
            with self.subTest(values=values, validator=type(inner_validator).__name__):
                validator = MultivalueValidator(inner_validator)
                if should_pass:
                    await validator(values)
                else:
                    with self.assertRaises(MappingValidationError):
                        await validator(values)


class TestMappingValidator(unittest.IsolatedAsyncioTestCase):
    """Тесты для MappingValidator"""

    async def test_all_fields_pass(self):
        validator = MappingValidator(
            name=Required(),
            email=Email(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        await validator({"name": "John", "email": "john@example.com"})

    async def test_single_field_fails(self):
        validator = MappingValidator(
            name=Required(),
            email=Email(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator({"name": "John", "email": "invalid"})
        errors = exc_info.exception.args[0]
        self.assertIn("email", errors)
        self.assertNotIn("name", errors)

    async def test_multiple_fields_fail(self):
        validator = MappingValidator(
            name=Required(),
            email=Email(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator({"name": None, "email": "invalid"})
        errors = exc_info.exception.args[0]
        self.assertIn("name", errors)
        self.assertIn("email", errors)

    async def test_missing_fields(self):
        validator = MappingValidator(
            name=Required(),
            age=Required(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        with self.assertRaises(MappingValidationError) as exc_info:
            await validator({})
        errors = exc_info.exception.args[0]
        self.assertIn("name", errors)
        self.assertIn("age", errors)

    async def test_extra_fields_ignored(self):
        validator = MappingValidator(
            name=Required(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        await validator({"name": "John", "extra": "ignored"})

    async def test_with_chain_validators(self):
        validator = MappingValidator(
            name=ChainValidator(Required(), Length(min_length=3, max_length=50)),
            email=ChainValidator(Required(), Email()),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        await validator({"name": "John", "email": "john@example.com"})

    async def test_dict_initialization(self):
        validators = {
            "name": Required(),
            "email": Email()
        }
        validator = MappingValidator(validators, attrgetter=lambda obj, attr: obj.get(attr))
        await validator({"name": "John", "email": "john@example.com"})

    async def test_kwargs_initialization(self):
        validator = MappingValidator(
            name=Required(),
            email=Email(),
            attrgetter=lambda obj, attr: obj.get(attr)
        )
        await validator({"name": "John", "email": "john@example.com"})

    async def test_empty_mapping(self):
        validator = MappingValidator({})
        await validator({})

    async def test_various_mappings(self):
        test_cases = [
            ({"name": "John", "age": 25}, True),
            ({"name": "Jo", "age": 25}, False),
            ({"name": "John", "age": 5}, False),
            ({"name": None, "age": 25}, False),
        ]
        for data, should_pass in test_cases:
            with self.subTest(data=data):
                validator = MappingValidator(
                    name=ChainValidator(Required(), Length(min_length=3, max_length=50)),
                    age=Number(minimum=18, maximum=100),
                    attrgetter=lambda obj, attr: obj.get(attr)
                )
                if should_pass:
                    await validator(data)
                else:
                    with self.assertRaises(MappingValidationError):
                        await validator(data)


class TestValidationErrorOperations(unittest.IsolatedAsyncioTestCase):
    """Тесты для операций с ValidationError"""

    async def test_add_validation_errors(self):
        error1 = ValidationError("Error 1")
        error2 = ValidationError("Error 2")
        result = error1 + error2
        self.assertIsInstance(result, ChainValidationError)

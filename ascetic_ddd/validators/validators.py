import numbers
import re
import collections.abc
import typing

from .exceptions import ValidationError, ChainValidationError, MappingValidationError

dummy_gettext = lambda v: v


class Validator:
    msg = 'Improper value "%s"'

    def __init__(self, msg=None):
        if msg is not None:
            self.msg = msg


class Required(Validator):
    empty_values = (None, '', [])
    msg = "The value is required"

    # TODO: accept Session obj and make it async?
    # TODO: accept gettext function?
    # Варианты:
    # - Result obj
    # - Self Shunt pattern
    #   - https://pdfs.semanticscholar.org/3e18/76a233963013d84bcba6a66289d623c63d57.pdf
    # - Notification Pattern
    #   - https://martinfowler.com/eaaDev/Notification.html
    #   - https://github.com/akornatskyy/goext/tree/master/validator
    # - Pydantic
    #   - https://github.com/pydantic/pydantic
    #   - https://github.com/pydantic/pydantic-core
    # - json-schema-validator
    #   - https://github.com/zyga/json-schema-validator
    # - LIVR
    #   - https://github.com/koorchik/LIVR
    #   - https://github.com/asholok/python-validator-livr
    # - FluentValidation
    #   - https://github.com/p-hzamora/FluentValidation
    #   - https://github.com/mariotaddeucci/fluent_validator
    async def __call__(self, value, gettext=dummy_gettext):
        if value in self.empty_values:
            raise ValidationError(gettext(self.msg))


class Regex(Validator):

    def __init__(self, regex=None, msg=None):
        if regex is not None:
            self.regex = regex
        super().__init__(msg)

    async def __call__(self, value, gettext=dummy_gettext):
        if not bool(self.regex.match(value)):
            raise ValidationError(gettext(self.msg), (value,))


class Email(Regex):
    regex = re.compile(r'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.(?:[A-Z]{2}|com|org|net|gov|mil|biz|info|mobi|name|aero|jobs|museum)$', re.I)


class Length(Validator):
    msg = 'Wrong length of the value "%s" (should be between %s and %s)'
    
    def __init__(self, min_length=1, max_length=None, msg=None):
        if max_length is not None:
            assert max_length >= min_length, "max_length must be greater than or equal to min_length"
        self.min_length = min_length
        self.max_length = max_length
        super().__init__(msg)

    async def __call__(self, value, gettext=dummy_gettext):
        length = len(value)
        if not (length >= self.min_length and
                (self.max_length is None or length <= self.max_length)):
            raise ValidationError(gettext(self.msg), (
                value,
                self.min_length,
                self.max_length or "any"
            ))


class Number(Validator):
    msg = 'Wrong value "%s" (should be between %s and %s)'

    def __init__(self, minimum=None, maximum=None, msg=None):
        if None not in (minimum, maximum):
            assert maximum >= minimum, "maximum must be greater than or equal to minimum"
        self.minimum = minimum
        self.maximum = maximum
        super().__init__(msg)

    async def __call__(self, value, gettext=dummy_gettext):
        if not (
                isinstance(value, numbers.Number) and
                (self.minimum is None or value >= self.minimum) and
                (self.maximum is None or value <= self.maximum)
        ):
            raise ValidationError(gettext(self.msg), (
                value,
                self.minimum or "any",
                self.maximum or "any"
            ))


class ChainValidator:
    def __init__(self, *validators):
        self.validators = validators

    async def __call__(self, value, gettext=dummy_gettext):
        errors = []
        for validator in self.validators:
            assert isinstance(validator, collections.abc.Callable), 'The validator must be callable'
            try:
                await validator(value, gettext=gettext)
            except ValidationError as e:
                errors.append(e)
                # Don't need message code. To rewrite message simple wrap (or extend) validator.
        if errors:
            raise ChainValidationError(errors)


class MultivalueValidator:
    """
    Combine with LengthValidator if necessary.
    """
    def __init__(self, validator):
        self.validator = validator

    async def __call__(self, values: typing.Collection, gettext=dummy_gettext):
        errors = {}
        for i, value in enumerate(values):
            try:
                await self.validator(value, gettext=gettext)
            except ValidationError as e:
                errors[i] = e
        if errors:
            raise MappingValidationError(errors)


class MappingValidator:
    """
    The problem: how to express non_field_errors?
    The solution is simple:
    {
        entity_attr: [
            {entity_error},
            {nested_attr: [{attr_error}]}
        ]
    }
    """
    attrgetter = staticmethod(lambda obj, attr: getattr(obj, attr, None))

    def __init__(self, *args, attrgetter=None, **kwargs):
        self.validators = kwargs or args[0]
        if attrgetter is not None:
            self.attrgetter = attrgetter

    async def __call__(self, items, gettext=dummy_gettext):
        errors = {}
        for name, validator in self.validators.items():
            try:
                await validator(self.attrgetter(items, name), gettext=gettext)
            except ValidationError as e:
                errors[name] = e
        if errors:
            raise MappingValidationError(errors)

    def __getitem__(self, item: str):
        return self.validators[item]

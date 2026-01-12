__all__ = (
    "ValidationError",
    "ChainValidationError",
    "MappingValidationError",
)


class ValidationError(ValueError):
    def __add__(self, other):
        return ChainValidationError([self, other])


class ChainValidationError(ValidationError):
    def __add__(self, other: 'ChainValidationError'):
        assert isinstance(other, ChainValidationError)
        errors = []
        for operand_error in (self.args[1], other.args[1]):
            errors.extend(operand_error)
        return type(self)(errors)


class MappingValidationError(ValidationError):
    def __add__(self, other: 'MappingValidationError'):
        assert isinstance(other, MappingValidationError)
        errors = {}
        for operand_error in (self.args[1], other.args[1]):
            for k, v in operand_error.items():
                if k not in errors:
                    errors[k] = v
                else:
                    errors[k] = errors[k] + v
        return type(self)(errors)

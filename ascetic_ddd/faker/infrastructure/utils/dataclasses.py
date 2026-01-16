import typing
import dataclasses

__all__ = ("IDataclass", "DataclassProtocol",)


@typing.runtime_checkable
class IDataclass(typing.Protocol):
    __dataclass_fields__: typing.ClassVar[typing.Dict[str, typing.Any]]
    # __dataclass_params__: typing.Dict
    # __post_init__: typing.Optional[typing.Callable]


@typing.runtime_checkable
@dataclasses.dataclass
class DataclassProtocol(typing.Protocol):
    pass

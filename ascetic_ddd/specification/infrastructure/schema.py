"""The foreign keys of a storage, for the queries of one table.

A schema is the foreign keys of a storage, as ``\\d`` shows them, and
nothing of any aggregate or query: a key is on a table, of columns, and
references a table's columns. What the compiler calls a row in a query - an
alias - is the compiler's own, made as it goes. The one thing of the query
in a schema is the table the query is of, ``FROM stores s``: the row the
compiler starts from, and what it qualifies that row's columns with.

A tree names a collection by the table its rows are in, ``store_items``,
or, where two keys of that table reference the same row, by the key's name;
and an object kept in a table of its own by the key's column, ``owner_id``.
A key has a name as it has in PostgreSQL: the one it is given, or
``<table>_<columns>_fkey``. A Value Object kept in the query's row as a
column of a composite type is declared as one, ``composite("stores",
"address")``, and a path through it from the candidate is a member of it,
``("s"."address")."city"``: from the candidate an undeclared name is a
table's alias, ``"s"."price"``.
"""
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class ForeignKey:
    """``table (columns) REFERENCES referenced_table (referenced_columns)``.

    A key has at least one column: without any, every row of the table
    would belong to every row it references; and as many referenced
    columns as columns. Any other is refused where it is declared.
    ``table`` is a table; or, for a key on a row of an array in a
    composite, which has no table, the array's column by its table,
    ``stores.items``.
    """
    table: str
    columns: List[str]
    referenced_table: str
    referenced_columns: List[str]
    constraint_name: Optional[str] = None

    def __post_init__(self) -> None:
        # A key that could not be created is a mistake in the program's
        # constants - a schema is declared, not read - and is refused where
        # it is declared, in PostgreSQL's words, rather than at the first
        # query that joins by it.
        if not self.columns:
            raise ValueError("%s: a foreign key has at least one column" % self._ddl())
        if len(self.columns) != len(self.referenced_columns):
            raise ValueError(
                "%s: number of referencing and referenced columns for foreign key disagree" % self._ddl()
            )

    def _ddl(self) -> str:
        """The key as ``\\d`` shows it."""
        return "%s (%s) REFERENCES %s (%s)" % (
            self.table, ", ".join(self.columns), self.referenced_table, ", ".join(self.referenced_columns),
        )

    @property
    def name(self) -> str:
        """The key's name: the one it was given, or the one PostgreSQL gives
        a key that was not, ``<table>_<columns>_fkey``."""
        if self.constraint_name is not None:
            return self.constraint_name
        table = self.table.rsplit(".", 1)[-1]
        return "%s_%s_fkey" % (table, "_".join(self.columns))


class SchemaRegistry:
    """
    The foreign keys of a storage, for the queries of one table.

    Usage:
        schema = (SchemaRegistry("accounts")
            .with_alias("a")
            .foreign_key("transfers", "from_account_id", "accounts", "id")
            .foreign_key("transfers", "to_account_id", "accounts", "id")
            .foreign_key("accounts", "owner_id", "owners", "id"))

    A tree names a collection by its table, ``any(transfers, ...)``, and
    where two keys of that table reference the row it is named from, by the
    key's name, ``transfers_from_account_id_fkey``; an object by the key's
    column, ``owner_id.name``.
    """

    def __init__(self, table: str):
        # The table the query is of: the row the compiler starts from.
        self._table = table
        self._alias = ""
        self._keys: List[ForeignKey] = []
        self._composites: List[Tuple[str, str]] = []

    @property
    def table(self) -> str:
        """The query's table, as given: what its row is to a key."""
        return self._table

    @property
    def alias(self) -> str:
        """The alias the query gives its table: ``s`` of ``FROM stores s``."""
        return self._alias

    def with_alias(self, alias: str) -> "SchemaRegistry":
        """The alias the query gives its table: ``s`` of ``FROM stores s``."""
        self._alias = alias
        return self

    def foreign_key(
        self,
        table: str,
        column: str,
        referenced_table: str,
        referenced_column: str,
        *,
        constraint_name: Optional[str] = None,
    ) -> "SchemaRegistry":
        """A key of one column: ``table (column) REFERENCES referenced_table (referenced_column)``."""
        return self.key(ForeignKey(table, [column], referenced_table, [referenced_column], constraint_name))

    def foreign_key_composite(
        self,
        table: str,
        columns: List[str],
        referenced_table: str,
        referenced_columns: List[str],
        *,
        constraint_name: Optional[str] = None,
    ) -> "SchemaRegistry":
        """A key of several columns, each referencing the column at its place."""
        return self.key(ForeignKey(table, columns, referenced_table, referenced_columns, constraint_name))

    def key(self, key: ForeignKey) -> "SchemaRegistry":
        """A key as built."""
        self._keys.append(key)
        return self

    def key_named(self, name: str) -> Optional[ForeignKey]:
        """Return the key called ``name``, if there is one."""
        for key in self._keys:
            if key.name == name:
                return key
        return None

    def keys_referencing(self, table: str, referenced_table: str) -> List[ForeignKey]:
        """Return the keys on ``table`` that reference ``referenced_table``."""
        return [
            key for key in self._keys
            if key.table == table and key.referenced_table == referenced_table
        ]

    def keys_on(self, table: str, column: str) -> List[ForeignKey]:
        """Return the keys on ``table`` that ``column`` is a column of."""
        return [key for key in self._keys if key.table == table and column in key.columns]

    def composite(self, table: str, column: str) -> "SchemaRegistry":
        """The column ``column`` of ``table`` is of a composite type: a Value
        Object kept in the row. From the candidate a path through it is a
        member of the composite, ``("s"."address")."city"``, where a name
        not declared is a table's alias, ``"s"."price"``."""
        self._composites.append((table, column))
        return self

    def is_composite(self, table: str, column: str) -> bool:
        """Return whether ``column`` of ``table`` is declared a composite."""
        return (table, column) in self._composites

    def row(self) -> str:
        """Return what the query calls its table's row: the alias, or the table."""
        return self._alias or self._table

"""Schema registry for mapping collection fields to storage."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class StorageType(Enum):
    """Defines how a collection is stored."""
    EMBEDDED = "embedded"      # Collection stored as JSONB/array in parent table
    RELATIONAL = "relational"  # Collection stored in a separate table


@dataclass
class ForeignKeyPair:
    """Represents a single FK column mapping."""
    child_column: str   # Column in the child table (e.g., "store_id", "tenant_id")
    parent_column: str  # Column in the parent table (e.g., "id", "tenant_id")


@dataclass
class CollectionMapping:
    """Defines how a collection field maps to storage."""
    storage: StorageType
    table: str = ""                              # Table name (only for RELATIONAL)
    foreign_keys: List[ForeignKeyPair] = field(default_factory=list)  # FK relationship
    alias: str = ""                              # Optional custom alias for subquery


class SchemaRegistry:
    """
    Holds collection mappings for a specific aggregate/repository.

    A collection is named by its whole path from the aggregate: the objects
    and the collections on the way, then its own name, joined with dots. It
    used to be named by its last name alone, so the items of a store and the
    items of a category were one collection with one table.

    Usage:
        schema = (SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "store_items", "store_id", "id")
            .register_relational("Categories", "categories", "store_id", "id")
            .register_relational("Categories.Items", "category_items", "category_id", "id")
            .register_embedded("Tags"))
    """

    def __init__(self, parent_table: str):
        self._parent_table = parent_table
        self._parent_alias = ""
        self._collections: Dict[str, CollectionMapping] = {}

    @property
    def parent_table(self) -> str:
        return self._parent_table

    @property
    def parent_alias(self) -> str:
        return self._parent_alias

    def with_parent_alias(self, alias: str) -> "SchemaRegistry":
        """Set the parent table alias."""
        self._parent_alias = alias
        return self

    def register_embedded(self, field_name: str) -> "SchemaRegistry":
        """Register a collection stored as embedded JSONB/array."""
        self._collections[field_name] = CollectionMapping(
            storage=StorageType.EMBEDDED
        )
        return self

    def register_relational(
        self,
        field_name: str,
        table: str,
        child_column: str,
        parent_column: str
    ) -> "SchemaRegistry":
        """Register a collection stored in a separate table with simple FK."""
        self._collections[field_name] = CollectionMapping(
            storage=StorageType.RELATIONAL,
            table=table,
            foreign_keys=[ForeignKeyPair(child_column, parent_column)]
        )
        return self

    def register_relational_composite(
        self,
        field_name: str,
        table: str,
        foreign_keys: List[ForeignKeyPair]
    ) -> "SchemaRegistry":
        """Register a collection with composite FK."""
        self._collections[field_name] = CollectionMapping(
            storage=StorageType.RELATIONAL,
            table=table,
            foreign_keys=foreign_keys
        )
        return self

    def register(self, field_name: str, mapping: CollectionMapping) -> "SchemaRegistry":
        """Register a collection with full mapping configuration."""
        self._collections[field_name] = mapping
        return self

    def get(self, field_name: str) -> Optional[CollectionMapping]:
        """Return the collection mapping for a field name."""
        return self._collections.get(field_name)

    def is_embedded(self, field_name: str) -> bool:
        """Return True if collection is stored as embedded JSONB/array."""
        mapping = self._collections.get(field_name)
        if mapping is None:
            # Default to embedded if not registered
            return True
        return mapping.storage == StorageType.EMBEDDED

    def is_relational(self, field_name: str) -> bool:
        """Return True if collection is stored in a separate table."""
        mapping = self._collections.get(field_name)
        if mapping is None:
            return False
        return mapping.storage == StorageType.RELATIONAL

    def get_parent_ref(self) -> str:
        """Return the reference to parent table (alias or table name)."""
        if self._parent_alias:
            return self._parent_alias
        return self._parent_table

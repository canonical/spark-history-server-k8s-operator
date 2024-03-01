import logging
from typing import MutableMapping, Dict, Optional, List, Tuple

from ops.model import (
    Application, Relation, Unit
)

from charms.data_platform_libs.v0.data_interfaces import DataRelation

logger = logging.getLogger(__name__)


class StateBase:
    """Base state object."""

    def __init__(
            self, relation: Relation | None, component: Unit | Application,
    ):
        self.relation = relation
        self.component = component

    @property
    def relation_data(self) -> MutableMapping[str, str]:
        """The raw relation data."""
        if not self.relation:
            return {}

        return self.relation.data[self.component]

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        if not self.relation:
            return

        self.relation_data.update(items)


class DataDict(dict):
    """Python Standard Library 'dict' - like representation of Relation Data."""

    def __init__(self, relation_data: "DataRelation", relation_id: int):
        self.relation_data = relation_data
        self.relation_id = relation_id

    def all_data(self) -> Dict[str, str]:
        """Return the full content of the Abstract Relation Data dictionary."""
        result = self.relation_data.fetch_my_relation_data([self.relation_id])
        try:
            result_remote = self.relation_data.fetch_relation_data(
                [self.relation_id])
        except NotImplementedError:
            result_remote = {}
        if result:
            result_remote.update(result)
        return result_remote.get(self.relation_id, {})

    def __setitem__(self, key: str, item: str) -> None:
        """Set an item of the Abstract Relation Data dictionary."""
        self.relation_data.update_relation_data(self.relation_id, {key: item})

    def __getitem__(self, key: str) -> Optional[str]:
        """Get an item of the Abstract Relation Data dictionary."""
        result = None
        if not (
        result := self.relation_data.fetch_my_relation_field(self.relation_id,
                                                             key)):
            try:
                result = self.relation_data.fetch_relation_field(
                    self.relation_id, key)
            except NotImplementedError:
                pass
        return result

    def __repr__(self) -> str:
        """String representation Abstract Relation Data dictionary."""
        return repr(self.all_data())

    def __len__(self) -> int:
        """Length of the Abstract Relation Data dictionary."""
        return len(self.all_data())

    def __delitem__(self, key: str) -> None:
        """Delete an item of the Abstract Relation Data dictionary."""
        self.relation_data.delete_relation_data(self.relation_id, [key])

    def has_key(self, k) -> bool:
        """Does the key exist in the Abstract Relation Data dictionary?"""
        return k in self.all_data()

    def update(self, items: dict):
        """Update the Abstract Relation Data dictionary."""
        self.relation_data.update_relation_data(self.relation_id, items)

    def keys(self) -> List[str]:
        """Keys of the Abstract Relation Data dictionary."""
        return list(self.all_data().keys())

    def values(self) -> List[str]:
        """Values of the Abstract Relation Data dictionary."""
        return list(self.all_data().values())

    def items(self) -> List[Tuple[str, str]]:
        """Items of the Abstract Relation Data dictionary."""
        return list(self.all_data().items())

    def pop(self, item: str) -> str:
        """Pop an item of the Abstract Relation Data dictionary."""
        result = self.relation_data.fetch_my_relation_field(self.relation_id,
                                                            item)
        if not result:
            raise KeyError(f"Item {item} doesn't exist.")
        self.relation_data.delete_relation_data(self.relation_id, [item])
        return result

    def __contains__(self, item: str) -> bool:
        """Does the Abstract Relation Data dictionary contain item?"""
        return item in self.all_data().values()

    def __iter__(self):
        """Iterate through the Abstract Relation Data dictionary."""
        return iter(self.all_data())

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Safely get an item of the Abstract Relation Data dictionary."""
        if result := self[key]:
            return result
        return default

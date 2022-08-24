from dataclasses import dataclass
from enum import Enum


class PropertyType(Enum):
    UNIQUE = 1
    NOT_UNIQUE = 2
    FULL_TEXT = 3


@dataclass(frozen=True)
class Record:
    rid: str
    record_id: int
    prop_uq: int
    prop_nuq: int
    prop_ftx: int

    def __str__(self) -> str:
        return f"{self.__dict__}"

    def next_uq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq + 1, self.prop_nuq, self.prop_ftx)

    def next_nuq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq, self.prop_nuq + 1, self.prop_ftx)

    def next_ftx(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq, self.prop_nuq, self.prop_ftx + 1)

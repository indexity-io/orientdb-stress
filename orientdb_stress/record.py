from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from orientdb_stress.schema import OdbClassDef, OdbIndexDef, OdbPropertyDef


class PropertyType(Enum):
    UNIQUE = 1
    NOT_UNIQUE = 2
    FULL_TEXT = 3

    @staticmethod
    def type_for(pt_name: str) -> "PropertyType":
        try:
            return PropertyType[pt_name]
        except KeyError as e:
            raise TypeError(f"Unknown PropertyType {pt_name}") from e

    @staticmethod
    def types_for(pt_names: Sequence[str]) -> Sequence["PropertyType"]:
        return [PropertyType.type_for(pt) for pt in pt_names]

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)


@dataclass(frozen=True)
class Record:

    SCHEMA = [
        OdbClassDef(
            "Record",
            [
                OdbPropertyDef("id", "INTEGER"),
                OdbPropertyDef("prop_uq", "INTEGER"),
                OdbPropertyDef("prop_nuq", "INTEGER"),
                OdbPropertyDef("prop_ftx", "STRING"),
            ],
            [
                OdbIndexDef("id", ["id"], "UNIQUE"),
                OdbIndexDef("prop_uq", ["id", "prop_uq"], "UNIQUE"),
                OdbIndexDef("prop_nuq", ["prop_nuq"], "NOTUNIQUE"),
                OdbIndexDef("prop_ftx", ["prop_ftx"], "FULLTEXT ENGINE LUCENE"),
            ],
        )
    ]

    rid: str
    record_id: int
    prop_uq: int
    prop_nuq: int
    prop_ftx: str

    def __str__(self) -> str:
        return f"{self.__dict__}"

    def next_uq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq + 1, self.prop_nuq, self.prop_ftx)

    def next_nuq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq, self.prop_nuq + 1, self.prop_ftx)

    def next_ftx(self) -> "Record":
        current_ftx = int(self.prop_ftx)
        return Record(self.rid, self.record_id, self.prop_uq, self.prop_nuq, str(current_ftx + 1))

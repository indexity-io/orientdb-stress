import logging
from typing import Optional, Sequence

from orientdb_stress_tester.orientdb import JsonObject, Odb
from orientdb_stress_tester.scenario import Scenario, ScenarioAware


class OdbPropertyDef:
    def __init__(self, name: str, datatype: str) -> None:
        self.name = name
        self.datatype = datatype


class OdbIndexDef:
    def __init__(self, name: str, properties: Sequence[str], index_type: str):
        self.name = name
        self.properties = properties
        self.index_type = index_type


class OdbClassDef:
    def __init__(
        self, name: str, properties: Optional[Sequence[OdbPropertyDef]] = None, indexes: Optional[Sequence[OdbIndexDef]] = None
    ):
        self.name = name
        self.properties = properties or []
        self.indexes = indexes or []


class OdbSchemaManager:
    def __init__(self, odb: Odb) -> None:
        self.odb = odb

    def ensure_class(self, ocd: OdbClassDef) -> None:
        logging.debug("Installing schema for class [%s]", ocd.name)
        classes = self.odb.list_classes()
        if ocd.name not in classes:
            self.odb.create_class(ocd.name)
        cls_info = self.odb.get_class(ocd.name)
        for p in ocd.properties:
            self._ensure_property(ocd, cls_info, p)
        for i in ocd.indexes:
            self._ensure_index(ocd, i)

    def _ensure_property(self, ocd: OdbClassDef, cls_info: JsonObject, opd: OdbPropertyDef) -> None:
        props = cls_info.get("properties")
        if props is None:
            props = []
        if not opd.name in props:
            self.odb.create_property(ocd.name, opd.name, opd.datatype)

    def _ensure_index(self, ocd: OdbClassDef, oid: OdbIndexDef) -> None:
        self.odb.create_index(ocd.name, oid.name, oid.properties, oid.index_type)


class OdbSchemaInstaller(ScenarioAware):
    def __init__(self, odb: Odb, classes: Sequence[OdbClassDef]) -> None:
        self.odb = odb
        self.classes = classes

    def on_scenario_begin(self, scenario: Scenario) -> None:
        logging.info("Installing [%s] schema", self.odb.database)
        self.odb.create_if_not_exist()
        sm = OdbSchemaManager(self.odb)
        for cls in self.classes:
            sm.ensure_class(cls)

    def on_scenario_end(self, scenario: Scenario) -> None:
        pass

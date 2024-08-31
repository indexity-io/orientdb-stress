import json
import logging
import traceback
import typing
from random import Random
from typing import Any, Optional, Sequence

import requests
import requests.exceptions
from requests.auth import HTTPBasicAuth

from orientdb_stress import timed
from orientdb_stress.core import OdbException

JsonObject = dict[str, Any]


class OdbRestException(OdbException):
    def __init__(self, server_root: str, code: int, message: str) -> None:
        super().__init__(server_root, code, message)
        self.server_root = server_root
        self.code = code
        self.message = message


class OdbOfflineException(OdbRestException):
    pass


class OdbUnauthorisedException(OdbRestException):
    pass


class OdbServer:
    def __init__(self, name: str, server_root: str, user: str, passwd: str) -> None:
        self.name = name
        self.server_root = server_root
        self.user = user
        self.passwd = passwd
        self.running = False

    def is_running(self) -> bool:
        return self.running

    def set_running(self, run: bool) -> None:
        self.running = run

    def invoke_rest(self, verb: str, path: str, post_data: Optional[str] = None) -> JsonObject:
        try:
            logging.debug("%s %s %s", verb, path, "" if post_data is None else post_data)
            response = requests.request(
                verb, f"{self.server_root}{path}", data=post_data, auth=HTTPBasicAuth(self.user, self.passwd), timeout=30
            )
        except requests.exceptions.ConnectionError as e:
            raise OdbRestException(self.server_root, 503, "Connection Error") from e
        logging.debug(response.content)
        if len(response.content) == 0:
            raise OdbRestException(self.server_root, 503, "No response")
        response_obj = json.loads(response.content)
        if isinstance(response_obj, dict):
            errs = response_obj.get("errors")
            if errs is not None:
                code = errs[0]["code"]
                msg = errs[0]["content"]
                if "com.orientechnologies.common.concur.OOfflineNodeException" in msg:
                    raise OdbOfflineException(self.server_root, code, msg)
                if "401 Unauthorized" in msg:
                    raise OdbUnauthorisedException(self.server_root, code, msg)
                raise OdbRestException(self.server_root, code, msg)

        return response_obj

    def cmd(self, database: str, query: str) -> JsonObject:
        return self.invoke_rest("POST", f"/command/{database}/sql/", post_data=query)

    def sql(self, database: str, query: str) -> JsonObject:
        return self.invoke_rest("GET", f"/query/{database}/sql/{query.replace(' ', '%20')}")

    def update(self, database: str, update_query: str) -> int:
        res = self.cmd(database, update_query)
        try:
            return res["result"][0]["count"]
        except Exception as e:
            raise OdbException("No result count in CMD response") from e

    def create_db(self, name: str) -> JsonObject:
        logging.debug("Creating db %s on %s", name, self.name)
        return self.invoke_rest("POST", f"/database/{name}/plocal")

    def create_db_if_not_exist(self, name: str) -> Optional[JsonObject]:
        dbs = self.list_dbs()
        if name not in dbs:
            return self.create_db(name)
        return None

    def get_db(self, name: str) -> JsonObject:
        return self.invoke_rest("GET", f"/database/{name}")

    def list_dbs(self) -> Sequence[str]:
        logging.debug("Listing DBs on %s", self.name)
        resp = self.invoke_rest("GET", "/listDatabases")
        logging.debug("DBs on %s: %s", self.name, resp)
        return resp["databases"]

    def is_available(self) -> bool:
        try:
            self.list_dbs()
            return True
        except OdbException:
            return False

    def wait_for_available(self, timeout: float) -> Optional[bool]:
        return timed.try_predicate_until(self.is_available, timeout)

    def is_distributed_available(self, server_names: Sequence[str]) -> bool:
        logging.debug("Checking [%s] for HA status of servers: %s", self.name, server_names)
        dbs = None
        try:
            dbs = self.list_dbs()
        except OdbException:
            logging.debug(traceback.format_exc())
            return False

        if not dbs:
            # EE has a /distributed API to do this, but CE requires a /command
            # so needs a DB to be specified (and OSystem won't work)
            # This could be during a cold start of a node that is still synchronising DBs from cluster
            logging.debug("OrientDB CE requires a database be created to monitor HA status, but none are present")
            return False

        try:
            raw_resp = self.cmd(dbs[0], "HA STATUS -servers")
            if not raw_resp["result"]:
                return False
            resp = raw_resp["result"][0]["servers"]
            logging.debug("HA server state: %s", resp)

            other_members = [mem.get("name") for mem in resp["members"] if mem.get("name") not in server_names]
            if other_members:
                logging.debug("Unexpected other members in HA status - will wait for them to go away")
                return False

            for server in server_names:
                member = next((mem for mem in resp["members"] if mem.get("name") == server), None)
                if not member:
                    logging.debug(
                        "Server [%s] not present in HA status on [%s]",
                        server,
                        self.name,
                    )
                    return False
                if member.get("status") != "ONLINE":
                    logging.debug("Server [%s] is not ONLINE on [%s]", server, self.name)
                    return False
                dbStatus = member.get("databasesStatus")
                if not dbStatus:
                    logging.debug("Server [%s] has no DB status on [%s]", server, self.name)
                    return False
                for db in dbs:
                    if dbStatus.get(db) != "ONLINE":
                        logging.debug(
                            "Server [%s] does not have db [%s] in ONLINE status on [%s]",
                            server,
                            db,
                            self.name,
                        )
                        return False
                logging.debug("Server [%s] is HA ready", server)
            logging.debug("All servers HA ready")

            return True
        except OdbException:
            logging.debug(traceback.format_exc())
            return False

    def wait_for_distributed(self, server_names: Sequence[str], timeout: float) -> Optional[bool]:
        logging.debug(
            "Waiting for HA status on [%s], expecting servers: %s",
            self.name,
            server_names,
        )
        res = timed.try_predicate_until(lambda: self.is_distributed_available(server_names), timeout)
        if res is None:
            logging.info("Timed out waiting for HA status on [%s]", self.name)
        return res


class OdbServerPool:
    def __init__(self, servers: Sequence[OdbServer], random: Random):
        self.servers = servers
        self.random = random
        self.server_names = [srv.name for srv in servers]
        self.log = logging.getLogger(type(self).__name__)

    def _running_servers(self) -> Sequence[OdbServer]:
        return [srv for srv in self.servers if srv.is_running()]

    def _servers(self, include_not_running: bool) -> Sequence[OdbServer]:
        if include_not_running:
            return self.servers
        return self._running_servers()

    def wait_for_available(self, timeout: float) -> Optional[bool]:
        res = timed.try_each_predicate_until(self._running_servers(), lambda srv: srv.is_available(), timeout) is not None
        if res:
            self.log.info("Server pool has reached active state")
        return res

    def wait_for_distributed(self, timeout: float) -> Optional[Sequence[bool]]:
        running_servers = self._running_servers()
        running_server_names = [srv.name for srv in running_servers]
        logging.info("Waiting for HA status, expecting servers: %s", running_server_names)
        res = timed.try_each_timed_until(
            running_servers,
            lambda srv, rem: srv.wait_for_distributed(running_server_names, timeout),
            timeout,
        )
        if res:
            self.log.info("Server pool has reached stable HA state")
        return res

    def size(self) -> int:
        return len(self.servers)

    def choose_first_server(self, include_not_running: bool = False) -> OdbServer:
        return self._servers(include_not_running)[0]

    def choose_last_server(self, include_not_running: bool = False) -> OdbServer:
        return self._servers(include_not_running)[-1]

    def choose_random_server(self, include_not_running: bool = False) -> OdbServer:
        return self.random.choice(self._servers(include_not_running))

    def choose_random_server_not(self, srv: OdbServer) -> OdbServer:
        while True:
            rs = self.choose_random_server()
            if not srv or (rs.name != srv.name):
                return rs

    def choose_next_server(self, current: OdbServer) -> OdbServer:
        running_servers = self._running_servers()
        current_index = next(
            (i for i, srv in enumerate(running_servers) if srv.name == current.name),
            None,
        )
        if current_index is None:
            return running_servers[0]
        return running_servers[(current_index + 1) % len(running_servers)]


class Odb:
    def __init__(self, server_pool: OdbServerPool, database: str) -> None:
        self.server_pool = server_pool
        self.database = database

    def _server(self) -> OdbServer:
        return self.server_pool.choose_random_server()

    def create_if_not_exist(self) -> Optional[JsonObject]:
        return self._server().create_db_if_not_exist(self.database)

    def cmd(self, query: str) -> JsonObject:
        return self._server().cmd(self.database, query)

    def sql(self, query: str) -> JsonObject:
        return self._server().cmd(self.database, query)

    def update(self, update_query: str) -> int:
        return self._server().update(self.database, update_query)

    def list_classes(self) -> Sequence[str]:
        return typing.cast(Sequence[str], [cls.get("name") for cls in self.get_classes()])

    def get_classes(self) -> Sequence[JsonObject]:
        return self._server().get_db(self.database)["classes"]

    def get_class(self, name: str) -> JsonObject:
        return self._server().invoke_rest("GET", f"/class/{self.database}/{name}")

    def create_class(self, name: str) -> JsonObject:
        return self._server().invoke_rest("POST", f"/class/{self.database}/{name}")

    def list_properties(self, class_name: str) -> Sequence[str]:
        return [prop.name for prop in self.get_class(class_name)["properties"]]

    def create_property(self, class_name: str, property_name: str, datatype: str) -> JsonObject:
        return self._server().invoke_rest("POST", f"/property/{self.database}/{class_name}/{property_name}/{datatype}")

    def create_index(self, class_name: str, index_name: str, index_props: Sequence[str], index_type: str) -> JsonObject:
        return self.cmd(
            f"CREATE INDEX {class_name}.{index_name} IF NOT EXISTS ON {class_name} ({','.join(index_props)}) {index_type}"
        )

import logging
import signal
import subprocess
from pathlib import Path
from typing import Text

from orientdb_stress.templates import Templates, Template


class Docker:
    LOG = logging.getLogger("Docker")

    @staticmethod
    def docker_cleanup_by_label(label: str, label_value: str) -> None:
        label_filter = f"label={label}={label_value}"
        subprocess.run(
            [f"docker container rm --force --volumes $(docker container ls --quiet --all --filter '{label_filter}') || true"],
            shell=True,
            capture_output=True,
        )
        subprocess.run(
            ["docker network prune -f || true"],
            shell=True,
            capture_output=True,
        )


class DockerCompose:
    LOG = logging.getLogger("DockerCompose")

    def __init__(self, docker_compose_file: Path):
        self.docker_compose_file = docker_compose_file
        self.base_docker_cmd = ["docker", "compose", "-f", str(self.docker_compose_file)]

    def _docker_compose_exec(self, command: str, *args: str) -> None:
        cmd = self.base_docker_cmd + [command] + list(args)
        DockerCompose.LOG.debug("Executing %s", cmd)
        try:
            subprocess.run(cmd, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            DockerCompose.LOG.info("Docker exec failed")
            DockerCompose.LOG.debug("Docker stdout: %s", e.stdout.decode("utf-8"))
            DockerCompose.LOG.debug("Docker stderr: %s", e.stderr.decode("utf-8"))
            raise e

    def logs(self, service: str) -> subprocess.Popen:
        cmd = self.base_docker_cmd + ["logs", service, "--follow", "--since", "300ms"]
        DockerCompose.LOG.debug("Executing %s", cmd)
        return subprocess.Popen[Text](
            cmd,
            text=True,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )

    def down_all(self) -> None:
        DockerCompose.LOG.info("Down all")
        self._docker_compose_exec("down")

    def start(self, service: str) -> None:
        DockerCompose.LOG.info("Up %s", service)
        self._docker_compose_exec("up", "-d", service)

    def stop(self, service: str) -> None:
        DockerCompose.LOG.info("Stop %s", service)
        self._docker_compose_exec("stop", service)

    def rm(self, service: str) -> None:
        DockerCompose.LOG.info("Remove %s", service)
        self._docker_compose_exec("rm", service)

    def restart(self, service: str) -> None:
        DockerCompose.LOG.info("Restart %s", service)
        self._docker_compose_exec("restart", service)

    def kill(self, service: str, kill_signal: signal.Signals) -> None:
        DockerCompose.LOG.info("Kill -s %s %s", kill_signal.name, service)
        self._docker_compose_exec("kill", "-s", kill_signal.name, service)


class DockerComposeTemplates:
    @staticmethod
    def load_template(dc_profile: str) -> Template:
        # Use Templates class to load the template
        template_name = f"{dc_profile}.yml.j2"
        return Templates.load_template(template_name)

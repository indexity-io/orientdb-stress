import logging
import signal
import subprocess


class DockerCompose:
    LOG = logging.getLogger("DockerCompose")

    @staticmethod
    def _docker_compose_exec(command: str, *args: str) -> None:
        cmd = ["docker", "compose", command] + list(args)
        subprocess.run(cmd, capture_output=True, check=True)

    def down_all(self) -> None:
        DockerCompose.LOG.info("Down all")
        DockerCompose._docker_compose_exec("down")

    def start(self, service: str) -> None:
        DockerCompose.LOG.info("Up %s", service)
        DockerCompose._docker_compose_exec("up", "-d", service)

    def stop(self, service: str) -> None:
        DockerCompose.LOG.info("Stop %s", service)
        DockerCompose._docker_compose_exec("stop", service)

    def rm(self, service: str) -> None:
        DockerCompose.LOG.info("Remove %s", service)
        DockerCompose._docker_compose_exec("rm", service)

    def restart(self, service: str) -> None:
        DockerCompose.LOG.info("Restart %s", service)
        DockerCompose._docker_compose_exec("restart", service)

    def kill(self, service: str, kill_signal: signal.Signals) -> None:
        DockerCompose.LOG.info("Kill -s %s %s", kill_signal.name, service)
        DockerCompose._docker_compose_exec("kill", "-s", kill_signal.name, service)

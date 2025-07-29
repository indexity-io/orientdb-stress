import logging
import signal
import subprocess


class DockerCompose:
    LOG = logging.getLogger("DockerCompose")

    @staticmethod
    def _docker_compose_exec(command: str, *args: str) -> None:
        cmd = ["docker", "compose", command] + list(args)
        DockerCompose.LOG.debug("Executing %s", cmd)
        try:
            subprocess.run(cmd, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            DockerCompose.LOG.info("Docker exec failed")
            DockerCompose.LOG.debug("Docker stdout: %s", e.stdout.decode("utf-8"))
            DockerCompose.LOG.debug("Docker stderr: %s", e.stderr.decode("utf-8"))
            raise e

    @staticmethod
    def down_all() -> None:
        DockerCompose.LOG.info("Down all")
        DockerCompose._docker_compose_exec("down")

    @staticmethod
    def start(service: str) -> None:
        DockerCompose.LOG.info("Up %s", service)
        DockerCompose._docker_compose_exec("up", "-d", service)

    @staticmethod
    def stop(service: str) -> None:
        DockerCompose.LOG.info("Stop %s", service)
        DockerCompose._docker_compose_exec("stop", service)

    @staticmethod
    def rm(service: str) -> None:
        DockerCompose.LOG.info("Remove %s", service)
        DockerCompose._docker_compose_exec("rm", service)

    @staticmethod
    def restart(service: str) -> None:
        DockerCompose.LOG.info("Restart %s", service)
        DockerCompose._docker_compose_exec("restart", service)

    @staticmethod
    def kill(service: str, kill_signal: signal.Signals) -> None:
        DockerCompose.LOG.info("Kill -s %s %s", kill_signal.name, service)
        DockerCompose._docker_compose_exec("kill", "-s", kill_signal.name, service)

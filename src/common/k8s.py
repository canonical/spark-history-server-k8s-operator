import logging
from abc import ABC

from ops import Container
from ops.pebble import ExecError
from typing_extensions import override

from common.workload import AbstractWorkload

logger = logging.getLogger(__name__)


class K8sWorkload(AbstractWorkload, ABC):
    container: Container

    def exists(self, path: str) -> bool:
        """Check for file existence.

        Args:
            path: the full filepath to write to
        """
        return self.container.exists(path)

    @override
    def read(self, path: str) -> list[str]:
        if not self.container.exists(path):
            return []  # FIXME previous return is None
        else:
            with self.container.pull(path) as f:
                content = f.read().split("\n")

        return content

    @override
    def write(self, content: str, path: str, mode: str = "w") -> None:
        if mode == "a":
            content = "\n".join(self.read(path) + [content])
        self.container.push(path, content, make_dirs=True)

    @override
    def exec(
            self, command: str, env: dict[str, str] | None = None,
            working_dir: str | None = None
    ) -> str:
        try:
            process = self.container.exec(
                command=command.split(),
                environment=env,
                working_dir=working_dir,
                combine_stderr=True,
            )
            output, _ = process.wait_output()
            return output
        except ExecError as e:
            logger.error(str(e.stderr))
            raise e

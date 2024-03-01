import re

from common.utils import WithLogging
from core.state import IngressUrl, S3ConnectionInfo
from core.workload import SparkHistoryWorkloadBase
from managers.tls import TLSManager


class HistoryServerConfig(WithLogging):
    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.eventLog.enabled": "true",
    }

    def __init__(
            self, s3: S3ConnectionInfo | None, ingress: IngressUrl | None
    ):
        self.s3 = s3
        self.ingress = ingress

    @staticmethod
    def _ssl_enabled(endpoint: str | None) -> str:
        """Check if ssl is enabled."""
        if (
                not endpoint or
                endpoint.startswith("https:") or
                ":443" in endpoint
        ):
            return "true"

        return "false"

    @property
    def _ingress_proxy_conf(self) -> dict[str, str]:
        return (
            {
                "spark.ui.proxyBase": self._ingress_pattern.sub("/",
                                                                ingress.url),
                "spark.ui.proxyRedirectUri": self._ingress_pattern.match(
                    ingress.url).group(),
            }
            if (ingress := self.ingress)
            else {}
        )

    @property
    def _s3_conf(self) -> dict[str, str]:
        if s3 := self.s3:
            return {
                "spark.hadoop.fs.s3a.endpoint": s3.endpoint or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": s3.access_key,
                "spark.hadoop.fs.s3a.secret.key": s3.secret_key,
                "spark.eventLog.dir": s3.log_dir,
                "spark.history.fs.logDirectory": s3.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(
                    s3.endpoint),
            }
        return {}

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_conf | self._s3_conf | self._ingress_proxy_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )


class HistoryServerManager(WithLogging):

    def __init__(
            self, workload: SparkHistoryWorkloadBase
    ):
        self.workload = workload

        self.tls = TLSManager(workload)

    def update(
            self, s3: S3ConnectionInfo | None, ingress: IngressUrl | None
    ) -> None:
        """Update the Spark History server service if needed."""
        self.workload.stop()

        config = HistoryServerConfig(s3, ingress)

        self.workload.write(
            config.contents, str(self.workload.paths.spark_properties)
        )

        self.tls.reset()

        if not s3:
            return

        if tls_ca_chain := s3.tls_ca_chain:
            self.tls.import_ca("\n".join(tls_ca_chain))

        self.workload.start()

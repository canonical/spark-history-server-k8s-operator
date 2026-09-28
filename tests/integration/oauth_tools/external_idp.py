#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

import abc
import logging
import os
import re
from os.path import join
from pathlib import Path

import requests
from lightkube import Client, KubeConfig, codecs
from lightkube.core.exceptions import ApiError
from lightkube.resources.apps_v1 import Deployment
from lightkube.resources.core_v1 import Namespace, Pod, Service
from playwright.sync_api import expect
from playwright.sync_api._generated import Page
from tenacity import retry, stop_after_attempt, wait_fixed

DEX_MANIFESTS = Path(__file__).parent / "dex.yaml"
KUBECONFIG = os.environ.get("TESTING_KUBECONFIG", "~/.kube/config")

DEX_CLIENT_ID = "client_id"
DEX_CLIENT_SECRET = "client_secret"

EXTERNAL_USER_EMAIL = "admin@example.com"
EXTERNAL_USER_PASSWORD = "password"

logger = logging.getLogger(__name__)


class ExternalIdpService(abc.ABC):
    """Abstract class for managing lifecycle for an external IdP."""

    @property
    @abc.abstractmethod
    def client_id(self) -> str:
        """The client_id of a registered client."""
        ...

    @property
    @abc.abstractmethod
    def client_secret(self) -> str:
        """The client_sercet of a registered client."""
        ...

    @property
    @abc.abstractmethod
    def user_email(self) -> str:
        """The test user's email."""
        ...

    @property
    @abc.abstractmethod
    def issuer_url(self) -> str:
        """The provider's issuer URL."""
        ...

    @abc.abstractmethod
    def create_idp_service(self) -> None:
        """Deploy and configure the idp service."""
        ...

    @abc.abstractmethod
    def remove_idp_service(self) -> None:
        """Remove and clean up the idp service."""
        ...

    @abc.abstractmethod
    def update_redirect_uri(self, redirect_uri: str) -> None:
        """Update the registered client's redirect_uri."""
        ...

    @abc.abstractmethod
    def complete_user_login(self, page: Page) -> None:
        """Get a page on the IDP login page and login the user."""
        ...


class DexIdpService(ExternalIdpService):
    """Class for managing lifecycle for an external Dex IdP."""

    client_id = DEX_CLIENT_ID
    client_secret = DEX_CLIENT_SECRET
    user_email = EXTERNAL_USER_EMAIL
    user_password = EXTERNAL_USER_PASSWORD
    _namespace = "dex"

    def __init__(self, client: Client | None = None):
        if not client:
            client = Client(config=KubeConfig.from_file(KUBECONFIG), field_manager="dex-test")
        self._client = client
        self._redirect_uri = ""
        if not self._dex_namespace_exists():
            self._apply_dex_resources()

    @property
    def issuer_url(self) -> str:
        """The provider's issuer URL."""
        service = self._client.get(Service, "dex", namespace=self.namespace)
        assert service.status is not None and service.status.loadBalancer is not None
        ingress = service.status.loadBalancer.ingress
        assert ingress
        return f"http://{ingress[0].ip}:5556/"

    @property
    def namespace(self) -> str:
        """The k8s namespace in which dex is deployed."""
        return self._namespace

    def _dex_namespace_exists(self) -> bool:
        try:
            self._client.get(Namespace, self.namespace)
            return True
        except ApiError:
            return False

    def _get_dex_manifest(self) -> list[codecs.AnyResource]:
        """Get the dex manifest with the correct parameters filled in."""
        temp_issuer_url = None
        try:
            temp_issuer_url = self.issuer_url
        except ApiError:
            logger.info("No service found for identity provider")

        temp_redirect_url: str | None = self._redirect_uri
        if not temp_redirect_url:
            temp_redirect_url = None

        with open(DEX_MANIFESTS, "r") as file:
            return codecs.load_all_yaml(
                file,
                context={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": temp_redirect_url,
                    "issuer_url": temp_issuer_url,
                    "namespace": self.namespace,
                },
            )

    def _restart_dex(self) -> list[str]:
        """Restart the dex pods."""
        deleted: list[str] = []
        for pod in self._client.list(Pod, namespace=self.namespace, labels={"app": "dex"}):
            assert pod.metadata is not None and pod.metadata.name is not None
            deleted.append(pod.metadata.name)
            self._client.delete(Pod, pod.metadata.name, namespace=self.namespace)
        return deleted

    def _apply_dex_resources(self) -> None:
        """Apply the dex manifest."""
        objs = self._get_dex_manifest()

        for obj in objs:
            self._client.apply(obj, force=True)

        logger.info("Restarting dex")
        deleted_pod_names = self._restart_dex()

        logger.info("Waiting for dex to be ready")
        self._wait_until_is_ready(ignore=deleted_pod_names)

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(10))
    def assert_pod_status(self, pod: Pod, status: str):
        """Assert that pod status is the desired one."""
        assert pod.metadata is not None and pod.metadata.name is not None
        fetched_pod = self._client.get(
            Pod,
            name=pod.metadata.name,
            # for_conditions=["Ready"],
            namespace=self.namespace,
        )
        assert fetched_pod.status is not None
        current_status = fetched_pod.status.to_dict()
        conditions = [c for c in current_status.get("conditions", []) if c["status"] == "True"]
        assert any(c["type"] == status for c in conditions)

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(10))
    def assert_deployment_status(self, deployment_name: str, status: str):
        """Assert that pod status is the desired one."""
        fetched_deployment = self._client.get(
            Deployment,
            name=deployment_name,
            # for_conditions=["Ready"],
            namespace=self.namespace,
        )
        assert fetched_deployment.status is not None
        current_status = fetched_deployment.status.to_dict()
        conditions = [c for c in current_status.get("conditions", []) if c["status"] == "True"]
        assert any(c["type"] == status for c in conditions)

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(10))
    def check_dex_health(self):
        """Check that the dex service is reachable."""
        resp = requests.get(join(self.issuer_url, ".well-known/openid-configuration"))
        if resp.status_code != 200:
            raise RuntimeError("Failed to deploy dex")

    def _wait_until_is_ready(self, ignore: list[str] | None = None) -> None:
        """Wait until the dex service is ready."""
        ignore = ignore or []
        for pod in self._client.list(Pod, namespace=self.namespace, labels={"app": "dex"}):
            # Some pods may be deleted, if we are restarting
            assert pod.metadata is not None and pod.metadata.name is not None
            if pod.metadata.name in ignore:
                continue
            # assert that dex pod is in the correct status
            self.assert_pod_status(pod, "Ready")
        # assert deployment is in the correct status
        self.assert_deployment_status("dex", "Available")
        # check that the dex service is reachable
        self.check_dex_health()

    def create_idp_service(self):
        """Deploy and configure the dex service."""
        self._apply_dex_resources()

    def update_redirect_uri(self, redirect_uri: str) -> None:
        """Update the registered client's redirect_uri."""
        if not redirect_uri:
            logger.info("Empty parameter for redirect_uri")
            return
        self._redirect_uri = redirect_uri
        self._apply_dex_resources()

    def remove_idp_service(self) -> None:
        """Remove and clean up the dex manifests."""
        logger.info("Deleting dex resources")
        for obj in self._get_dex_manifest():
            metadata = obj.metadata
            assert (
                metadata is not None
                and metadata.name is not None
                and metadata.namespace is not None
            )
            try:
                # `type(obj)` is dynamic, so its exact NamespacedResource type can't be verified statically.
                self._client.delete(type(obj), metadata.name, namespace=metadata.namespace)  # type: ignore[type-var]
            except ApiError:
                pass

    def complete_user_login(self, page: Page) -> None:
        """Get a page on the IDP login page and login the user."""
        logger.info("Signing in to dex")
        expect(page).to_have_url(re.compile(rf"{self.issuer_url}*"))
        page.get_by_placeholder("email address").click()
        page.get_by_placeholder("email address").fill(self.user_email)
        page.get_by_placeholder("password").click()
        page.get_by_placeholder("password").fill(self.user_password)
        page.get_by_role("button", name="Login").click()

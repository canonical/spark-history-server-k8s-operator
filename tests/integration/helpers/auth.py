#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging

import jubilant
from playwright.sync_api import BrowserContext, Page
from tenacity import Retrying, stop_after_attempt, wait_fixed

from ..oauth_tools.external_idp import ExternalIdpService
from ..types import IngressMode, IntegrationTestsCharms
from .history_server import APP_NAME

logger = logging.getLogger(__name__)


def deploy_identity_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    ingress_mode: IngressMode = IngressMode.TRAEFIK,
):
    """Deploy the identity setup for enabling authentication for the History Server."""
    # Deploy all charms necessary for Oauth2proxy integration
    juju.deploy(**charm_versions.oauth2proxy.deploy_dict())
    juju.deploy(**charm_versions.postgresql.deploy_dict())
    juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())
    juju.deploy(**charm_versions.hydra.deploy_dict())
    juju.deploy(**charm_versions.kratos.deploy_dict())
    juju.deploy(**charm_versions.identity_platform_login_ui_operator.deploy_dict())
    juju.deploy(**charm_versions.kratos_external_idp_integrator.deploy_dict())
    hserver_ingress_charm = charm_versions.ingress
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        hserver_ingress_charm = charm_versions.istio_ingress
        # Traefik ingress is needed for IAM bundle anyway
        juju.deploy(**charm_versions.ingress.deploy_dict())

    juju.integrate(
        charm_versions.self_signed_certificate.application_name,
        f"{charm_versions.ingress.application_name}:certificates",
    )
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        juju.integrate(
            charm_versions.self_signed_certificate.application_name,
            f"{hserver_ingress_charm.application_name}:certificates",
        )

    # hydra integrations
    juju.integrate(
        charm_versions.hydra.application_name, charm_versions.postgresql.application_name
    )
    juju.integrate(
        f"{charm_versions.hydra.application_name}:public-route",
        charm_versions.ingress.application_name,
    )

    # kratos integrations
    juju.integrate(
        charm_versions.kratos.application_name, charm_versions.postgresql.application_name
    )
    juju.integrate(
        f"{charm_versions.kratos.application_name}:public-route",
        charm_versions.ingress.application_name,
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.hydra.application_name}:hydra-endpoint-info",
    )

    # login ui integrations
    juju.integrate(
        charm_versions.hydra.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:ui-endpoint-info",
    )
    juju.integrate(
        charm_versions.hydra.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:hydra-endpoint-info",
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:ui-endpoint-info",
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:kratos-info",
    )
    juju.integrate(
        charm_versions.identity_platform_login_ui_operator.application_name,
        charm_versions.ingress.application_name,
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        charm_versions.kratos_external_idp_integrator.application_name,
    )

    # wait for all charms to be active/blocking
    juju.wait(
        lambda status: (
            jubilant.all_active(
                status,
                charm_versions.postgresql.application_name,
                charm_versions.self_signed_certificate.application_name,
                charm_versions.hydra.application_name,
                charm_versions.kratos.application_name,
                charm_versions.identity_platform_login_ui_operator.application_name,
            )
            and jubilant.all_blocked(
                status,
                charm_versions.kratos_external_idp_integrator.application_name,
            )
        ),
        delay=10,
        timeout=2000,
    )

    juju.config(
        charm_versions.kratos_external_idp_integrator.application_name,
        {
            "client_id": external_idp_service.client_id,
            "client_secret": external_idp_service.client_secret,
            "provider": "generic",
            "issuer_url": external_idp_service.issuer_url,
            "scope": "profile email",
            "provider_id": "Dex",
        },
    )
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.ingress.application_name,
            charm_versions.postgresql.application_name,
            charm_versions.self_signed_certificate.application_name,
            charm_versions.hydra.application_name,
            charm_versions.kratos.application_name,
            charm_versions.identity_platform_login_ui_operator.application_name,
            charm_versions.kratos_external_idp_integrator.application_name,
        ),
        delay=10,
        timeout=600,
    )

    # oauth2proxy integrations
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:receive-ca-cert",
        charm_versions.self_signed_certificate.application_name,
    )
    juju.wait(
        lambda status: (
            jubilant.all_active(
                status,
                charm_versions.oauth2proxy.application_name,
                charm_versions.self_signed_certificate.application_name,
            )
            and jubilant.all_agents_idle(status)
        ),
        delay=10,
        timeout=600,
    )

    oauth2proxy_ingress_relation_name = "ingress"
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        oauth2proxy_ingress_relation_name = "ingress-unauthenticated"
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:ingress",
        f"{hserver_ingress_charm.application_name}:{oauth2proxy_ingress_relation_name}",
    )
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:oauth",
        charm_versions.hydra.application_name,
    )

    forward_auth_relation = "forward-auth"
    if ingress_mode == IngressMode.TRAEFIK:
        juju.config(
            hserver_ingress_charm.application_name, {"enable_experimental_forward_auth": "True"}
        )
        forward_auth_relation = "experimental-forward-auth"
    juju.integrate(
        f"{hserver_ingress_charm.application_name}:{forward_auth_relation}",
        f"{charm_versions.oauth2proxy.application_name}:forward-auth",
    )
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        juju.integrate(
            f"{hserver_ingress_charm.application_name}:istio-ingress-config",
            f"{charm_versions.istio.application_name}:istio-ingress-config",
        )
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            hserver_ingress_charm.application_name,
        ),
        delay=10,
        timeout=600,
    )

    juju.integrate(charm_versions.oauth2proxy.application_name, f"{APP_NAME}:oauth2-proxy")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            charm_versions.ingress.application_name,
            hserver_ingress_charm.application_name,
            charm_versions.postgresql.application_name,
            charm_versions.self_signed_certificate.application_name,
            charm_versions.hydra.application_name,
            charm_versions.kratos.application_name,
            charm_versions.identity_platform_login_ui_operator.application_name,
            charm_versions.kratos_external_idp_integrator.application_name,
        ),
        delay=30,
        timeout=600,
    )

    task = juju.run(
        f"{charm_versions.kratos_external_idp_integrator.application_name}/0", "get-redirect-uri"
    )
    assert task.return_code == 0

    logger.info("Configuring the external provider")
    external_idp_service.update_redirect_uri(redirect_uri=task.results["redirect-uri"])

    logger.info("IAM bundle deployed successfully.")


def complete_authentication_flow(
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
    history_server_url: str,
):
    """Complete the OAuth2 authentication flow for the History Server."""
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(10)):
        with attempt:
            logger.info(f"Navigating to {history_server_url}")
            page.goto(history_server_url)
            logger.info("Clicking on Sign in,  with Generic identity provider...")
            with page.expect_navigation(timeout=30_000):
                page.get_by_text("Sign in with Generic").click(timeout=30_000)

            logger.info("Completing login in the external identity provider...")
            with page.expect_navigation(timeout=30_000):
                external_idp_service.complete_user_login(page)
            logger.info(f"Login flow completed: {page.url}")

            logger.info("Verifying the correct redirect after login")
            page.wait_for_url(history_server_url, timeout=30_000)

    logger.info("Verifying that the login flow was successful...")
    # The test uses Spark history server's /api/user endpoint to verify the session cookie is valid
    history_server_session_cookie = next(
        iter(
            [cookie for cookie in context.cookies() if cookie.get("name", None) == "_oauth2_proxy"]
        ),
        None,
    )
    assert history_server_session_cookie is not None, "Session cookie '_oauth2_proxy' not found"

    return history_server_session_cookie.get("value")

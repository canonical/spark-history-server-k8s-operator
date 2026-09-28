#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
import subprocess

import jubilant


def get_unit_address(
    juju: jubilant.Juju,
    app_name: str,
    unit_number: int = 0,
) -> str:
    """Retrieve the IP address of a specific unit of an application."""
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_number}"].address
    return address


def get_application_data(juju: jubilant.Juju, app_name: str, relation_name: str) -> dict:
    """Retrieves the application data from a specific relation.

    Args:
        juju: The Juju client object used to execute CLI commands.
        app_name: The name of the Juju application.
        relation_name: The name of the relation endpoint to query.

    Returns:
        A dictionary containing the application data for the specified relation.

    Raises:
        ValueError: If no relation data can be found for the specified
            relation endpoint.
    """
    unit_name = f"{app_name}/0"
    command_stdout = juju.cli("show-unit", unit_name, "--format=json")
    result = json.loads(command_stdout)
    relation_data = [
        v for v in result[unit_name]["relation-info"] if v["endpoint"] == relation_name
    ]
    if len(relation_data) == 0:
        raise ValueError(
            f"No relation data could be grabbed on relation with endpoint {relation_name}"
        )
    return {relation["relation-id"]: relation["application-data"] for relation in relation_data}


def get_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        f"-n{model}",
        f"-lapp.kubernetes.io/name={application_name}",
        "--no-headers",
        "-o=custom-columns=NAME:.metadata.name",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
    )
    stdout = proc.stdout.decode("utf8")
    return stdout.split()

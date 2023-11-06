#!/usr/bin/env python
import argparse
from pathlib import Path

from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from .. import (
    validate_inputs,
    get_avd_facts,
    get_device_structured_config,
    validate_structured_config,
    get_device_config,
)

from concurrent.futures import ProcessPoolExecutor, as_completed


def build_structured_config(hostname: str, inputs: dict, avd_facts: dict):
    results = validate_inputs(inputs)
    if results.failed:
        for result in results.validation_errors:
            print(result, flush=True)
        raise RuntimeError(f"{hostname} validate_inputs failed")

    structured_config = get_device_structured_config(
        hostname, inputs, avd_facts=avd_facts
    )

    return hostname, structured_config


def build_designed_config(hostname: str, structured_config: dict):
    results = validate_structured_config(structured_config)
    if results.failed:
        for result in results.validation_errors:
            print(result, flush=True)
        raise RuntimeError(f"{hostname} validate_structured_config failed")

    return hostname, get_device_config(structured_config)


def build(inventory_path: Path, intended_configs_path: Path, max_workers: int = 10):
    inventory = InventoryManager(loader=DataLoader(), sources=[inventory_path.as_posix()])
    variable_manager = VariableManager(loader=DataLoader(), inventory=inventory)

    all_hostvars = {}

    for hostname in inventory.hosts:
        if hostname == "cvp":
            continue
        all_hostvars[hostname] = variable_manager.get_vars(host=inventory.get_host(hostname))

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        # Generate facts
        avd_facts = get_avd_facts(all_hostvars)

        # Build structured config
        structured_configs = {}
        futures = [
            pool.submit(build_structured_config, hostname, hostvars, avd_facts)
            for hostname, hostvars in all_hostvars.items()
        ]
        for future in as_completed(futures):
            hostname, structured_config = future.result()
            structured_configs[hostname] = structured_config

        # Build designed config
        futures = [
            pool.submit(build_designed_config, hostname, structured_config)
            for hostname, structured_config in structured_configs.items()
        ]

        # Write config
        for future in as_completed(futures):
            hostname, designed_config = future.result()

            with open(intended_configs_path / f"{hostname}.cfg", "w") as f:
                f.write(designed_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build AVD fabric.")
    parser.add_argument("-i", "--inventory-path", required=True, type=Path)
    parser.add_argument("-o", "--intended-configs-path", default=Path("intended", "configs"), type=Path)
    parser.add_argument("-m", "--max-workers", nargs="?", default=10, type=int)

    args = parser.parse_args()

    inventory_path = args.inventory_path
    intended_configs_path = args.intended_configs_path
    max_workers = args.max_workers

    build(
        inventory_path=inventory_path,
        intended_configs_path=intended_configs_path,
        max_workers=max_workers
    )

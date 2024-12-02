#!/usr/bin/env python3
from __future__ import annotations

import yaml
import subprocess
from dataclasses import dataclass, field
import re
import os
import glob
import itertools
import argparse
from typing import Dict, List, Optional, Tuple
from collections.abc import Iterable

from slurmer.params import ParameterDict, Parameters, split_variables_and_arguments, normalize_parameters
from slurmer.utils import print_output


def normalize_slurm(slurm: str | Dict[str, str]) -> str:
    """Convert slurm parameters to a standardized string format."""
    if isinstance(slurm, str):
        return slurm
    return " ".join(
        (f"{k}={v}" if k.startswith("--") else f"{k} {v}")
        for k, v in slurm.items()
    )


@dataclass
class Grid:
    name: str

    script: str | None = None
    command: str | None = None

    env: str | None = None

    params: Parameters | List[Parameters] = field(default_factory=list)

    slurm: str | Dict[str, str] = ""

    completion: str | None = None
    precondition: str | None = None

    chain: int = 1
    dependency: str | List[str] = field(default_factory=list)


    def __post_init__(self):
        # Normalize parameters to a list of dictionaries which always has at least one element
        self.params = list(normalize_parameters(self.params)) or [{}]
        self.slurm = normalize_slurm(self.slurm)
        if isinstance(self.dependency, str) and self.dependency:
            self.dependency = [self.dependency]

    def should_skip(self, param_dict: ParameterDict, return_reason=False) -> bool | Tuple[bool, str]:
        skip, reason = False, ""
        if self.precondition:
            precond_path = self.precondition.format(**param_dict)
            if not os.path.exists(os.path.expanduser(precond_path)):
                skip, reason = True, f"Precondition {precond_path} does not exist"

        if self.completion:
            completion_path = self.completion.format(**param_dict)
            if os.path.exists(os.path.expanduser(completion_path)):
                skip, reason = True, f"Completion check file exists at {completion_path}"

        return (skip, reason) if return_reason else skip

    def job_name(self, param_dict: ParameterDict) -> str:
        return self.name.format(**param_dict)


class JobSubmitter:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            raw_config = yaml.safe_load(f)
            self.grids: Dict[str, Grid] = {
                name: Grid(**{"name": name, **config})
                for name, config in raw_config.items()
            }
        self.job_ids: Dict[str, List[str]] = {}
        self.running_jobs = self.get_running_jobs()

    def get_running_jobs(self) -> List[str]:
        """Get list of job names currently running for the current user."""
        result = subprocess.run(
            ["squeue", "-u", os.environ["USER"], "-h", "-o", "%j"],
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split('\n') if result.stdout.strip() else []

    def get_dependency_string(self,
                              grid: Grid,
                            previous_job_id: Optional[str] = None) -> str:
        """Generate dependency string for SLURM job."""
        dependency_parts = []

        # Handle chain dependencies
        if previous_job_id:
            dependency_parts.append(f"afterany:{previous_job_id}")

        # Handle cross-grid dependencies
        for dep_grid in grid.dependency:
            if dep_grid in self.job_ids:
                dep_jobs = ":".join(self.job_ids[dep_grid])
                dependency_parts.append(f"afterok:{dep_jobs}")

        if dependency_parts:
            return "--dependency=" + ",".join(dependency_parts)
        return ""

    def format_command(self,
                       grid: Grid,
                       param_dict: ParameterDict,
                       job_name: str,
                       previous_job_id: Optional[str] = None,
                       interactive: bool = False) -> str:
        """Format the complete command for job submission."""
        cmd_parts = []

        # Add conda environment activation if specified
        if grid.env:
            cmd_parts.append(f"source ~/.bashrc && conda activate {grid.env} &&")

        variables, arguments = split_variables_and_arguments(param_dict)
        for key, value in variables.items():
            cmd_parts.append(f'{key}="{value}"')

        if interactive:
            cmd_parts.append(grid.command if grid.command else "bash -l " + grid.script)
        else:
            # Handle direct command execution vs script
            cmd_parts.append("sbatch")
            # Add SLURM parameters
            if grid.slurm:
                cmd_parts.extend(grid.slurm.split())

            # Add dependency parameters
            dep_string = self.get_dependency_string(grid, previous_job_id)
            if dep_string:
                cmd_parts.append(dep_string)

            # Add job name
            cmd_parts.extend(["-J", job_name])

            cmd_parts.append((r"<<<EOF\n#!\bin\bash -l\n" + grid.command) if grid.command else grid.script)

        # Sort parameters to ensure consistent ordering (positional args first)
        sorted_arguments = sorted(arguments.items(), key=lambda x: (not x[0].startswith('$'), x[0]))
        for key, value in sorted_arguments:
            if key.startswith('$'):
                cmd_parts.append(str(value))
            elif value is None:
                cmd_parts.append(key)
            else:
                cmd_parts.extend([key, f'"{value}"'])

        if grid.command and not interactive:
            cmd_parts.append("\nEOF")

        return " ".join(cmd_parts)

    def submit_job(self,
                   cmd: str,
                   grid_name: str,
                   dry_run: bool = False) -> str:
        """Submit a job and return its ID."""

        if dry_run:
            print_output(cmd, color=None, stdout=True)
            return "-1"
        else:
            print_output(cmd, color=None, stdout=False)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print_output(result.stdout.rstrip(), color="green", stdout=True)
            job_id_match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if job_id_match:
                job_id = job_id_match.group(1)
                if grid_name not in self.job_ids:
                    self.job_ids[grid_name] = []
                self.job_ids[grid_name].append(job_id)
                return job_id
        else:
            raise RuntimeError(f"Error submitting job: {result.stderr}")

    def submit_grid(self, grid_name: str, dry_run: bool = False, interactive: bool = False):
        """Submit all jobs for a single grid."""

        grid = self.grids[grid_name]
        if all(grid.should_skip(param_dict) for param_dict in grid.params):
            print_output(f"{grid_name}: Skipping {len(grid.params)} jobs", color="red", stdout=False)
            return

        for param_dict in grid.params:
            job_name = grid.job_name(param_dict)

            skip, reason = grid.should_skip(param_dict, return_reason=True)
            if skip:
                print_output(f"{job_name}: {reason}", color="red", stdout=False)
                continue

            if job_name in self.running_jobs:
                print_output(f"{job_name}: Job already running", color="red", stdout=False)
                continue

            # Handle job chaining
            previous_job_id = None
            for _ in range(grid.chain):
                cmd = self.format_command(grid, param_dict, job_name, previous_job_id, interactive)
                job_id = self.submit_job(cmd, grid.name, dry_run)
                if job_id:
                    previous_job_id = job_id


    def submit_all(self, selection: list, dry_run: bool = False, interactive: bool = False):
        """Submit all grids in the configuration."""
        if not selection:
            for grid_name in self.grids:
                self.submit_grid(grid_name, dry_run, interactive)
        else:
            for grid_name in selection:
                if grid_name not in self.grids:
                    raise ValueError(f"Grid {grid_name} not found in configuration")

                self.submit_grid(grid_name, dry_run, interactive)

def main():
    parser = argparse.ArgumentParser(description="Submit SLURM jobs based on YAML configuration")
    parser.add_argument("config", help="Path to YAML configuration file")
    parser.add_argument("grids", nargs="*", help="Execute only the specified grids")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Print commands without submitting")
    parser.add_argument("-i", "--interactive", action="store_true", help="Get interactive commands instead")
    args = parser.parse_args()

    submitter = JobSubmitter(args.config)
    submitter.submit_all(args.grids, dry_run=args.dry_run, interactive=args.interactive)


if __name__ == "__main__":
    main()

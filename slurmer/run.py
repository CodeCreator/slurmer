#!/usr/bin/env python3
from __future__ import annotations

import yaml
import subprocess
from dataclasses import dataclass, field
import os
import re
import argparse
from typing import Dict, List, Optional, Tuple

from slurmer.params import ParameterDict, Parameters, split_variables_and_arguments, normalize_parameters, format_parameter
from slurmer.utils import print_output, unsafe_format





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

    def __post_init__(self):
        # Normalize parameters to a list of dictionaries which always has at least one element
        self.params = list(normalize_parameters(self.params)) or [{}]
        self.slurm = normalize_slurm(self.slurm)

    def skip_reason(self, param_dict: ParameterDict) -> str:
        if self.precondition:
            precond_path = unsafe_format(self.precondition, **param_dict)
            if not os.path.exists(os.path.expanduser(precond_path)):
                return "Missing Preconditon"

        if self.completion:
            completion_path = unsafe_format(self.completion, **param_dict)
            if os.path.exists(os.path.expanduser(completion_path)):
                return "Already Completed"

        return "None"

    def job_name(self, param_dict: ParameterDict) -> str:
        return unsafe_format(self.name, **param_dict)


class JobSubmitter:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.submitted_jobs = self.get_job_queue()

    def get_job_queue(self) -> List[str]:
        """Get list of job names currently queued or runnning for the current user."""
        result = subprocess.run(
            ["squeue", "-u", os.environ["USER"], "-h", "-o", "%j"],
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split('\n') if result.stdout.strip() else []

    def format_command(self,
                       grid: Grid,
                       param_dict: ParameterDict,
                       job_name: str,
                       previous_job_id: Optional[str] = None,
                       interactive: bool = False,
                       slurm_overrides: Optional[List[str]] = None) -> str:
        """Format the complete command for job submission."""
        cmd_parts = []

        # Add conda environment activation if specified
        if grid.env:
            cmd_parts.append(f"source ~/.bashrc && conda activate {grid.env} &&")

        variables, arguments = split_variables_and_arguments(param_dict)
        for key, value in variables.items():
            cmd_parts.append(f'{key}="{format_parameter(value)}"')

        if interactive:
            cmd_parts.append(grid.command if grid.command else "bash -l " + grid.script)
        else:
            # Handle direct command execution vs script
            cmd_parts.append("sbatch")
            # Add SLURM parameters
            if grid.slurm:
                cmd_parts.extend(grid.slurm.split())

            # Handle dependency chaining
            if previous_job_id:
                cmd_parts.append(f"--dependency=afterany:{previous_job_id}")

            # Add job name
            cmd_parts.extend(["-J", job_name])

            if slurm_overrides: 
                cmd_parts.extend(slurm_overrides)

            cmd_parts.append((r"<<<EOF\n#!\bin\bash -l\n" + grid.command) if grid.command else grid.script)

        # Sort parameters to ensure consistent ordering (positional args first)
        sorted_arguments = sorted(arguments.items(), key=lambda x: (not x[0].startswith('$'), x[0]))
        for key, value in sorted_arguments:
            if key.startswith('$'):
                cmd_parts.append(f"'{format_parameter(value)}'")
            elif value is None:
                cmd_parts.append(key)
            else:
                cmd_parts.extend([key, f'"{format_parameter(value)}"'])

        if grid.command and not interactive:
            cmd_parts.append("\nEOF")

        return " ".join(cmd_parts)

    def submit_job(self,
                   cmd: str,
                   dry_run: bool = False) -> str:
        """Submit a job and return its ID."""

        if dry_run:
            print_output(cmd, stdout=True)
            return "-1"
        else:
            print_output(cmd, stdout=False)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print_output(result.stdout.rstrip(), stdout=True)
            job_id_match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if job_id_match:
                job_id = job_id_match.group(1)
                return job_id
        else:
            raise RuntimeError(f"Error submitting job: {result.stderr}")

    def submit_grid(self, grid_id: str, dry_run: bool = False, interactive: bool = False, slurm_overrides: Optional[List[str]] = None):
        """Submit all jobs for a single grid."""

        grid_config = self.config[grid_id]
        if "name" not in grid_config:
            grid_config["name"] = grid_id

        grid = Grid(**grid_config)
        print_output(f"[{grid_id}]", color="yellow", stdout=False)

        skip_reasons = []
        param_dicts_to_run = []
        for param_dict in grid.params:
            skip_reason = grid.skip_reason(param_dict)
            if grid.job_name(param_dict) in self.submitted_jobs:
                skip_reason = "Job already submitted"

            skip_reasons.append(skip_reason)
            if skip_reason == "None":
                param_dicts_to_run.append(param_dict)

        counts = {reason: skip_reasons.count(reason) for reason in set(skip_reasons)}
        counts_formatted = ", ".join(f"{k}: {v}" for k, v in counts.items())
        num_skipping = sum(counts.values()) - counts.get("None", 0)
        print_output(f" - Skipping {num_skipping} jobs ({counts_formatted})", color="yellow", stdout=False)

        for param_dict in param_dicts_to_run:
            # Handle job chaining
            previous_job_id = None
            for _ in range(grid.chain):
                cmd = self.format_command(grid, param_dict, grid.job_name(param_dict), previous_job_id, interactive, slurm_overrides)
                job_id = self.submit_job(cmd, dry_run)
                if job_id:
                    previous_job_id = job_id

    def submit_many(self, selection: list, dry_run: bool = False, interactive: bool = False, slurm_overrides: Optional[List[str]] = None):
        """Submit all grids in the configuration."""
        if not selection:
            selection = list(self.config.keys())

        for grid_id in selection:
            if grid_id not in self.config:
                raise ValueError(f"Grid {grid_id} not found in configuration")

            self.submit_grid(grid_id, dry_run, interactive, slurm_overrides)

def main():
    parser = argparse.ArgumentParser(description="Submit SLURM jobs based on YAML configuration")
    parser.add_argument("grids", nargs="*", help="Execute only the specified grids")
    parser.add_argument("-c", "--config", default="runs.yaml", help="Path to YAML configuration file")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Print commands without submitting")
    parser.add_argument("-i", "--interactive", action="store_true", help="Get interactive commands instead")
    parser.add_argument("--slurm-arg", type=str, nargs="*", help="Override slurm commands globally")
    args = parser.parse_args()

    submitter = JobSubmitter(args.config)
    submitter.submit_many(args.grids, dry_run=args.dry_run, interactive=args.interactive, slurm_overrides=args.slurm_arg)


if __name__ == "__main__":
    main()

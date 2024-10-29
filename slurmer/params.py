#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field
import glob
import itertools
import os
from typing import Dict, List, Optional, Tuple
from collections.abc import Iterable

ParameterValue = str | int | float | bool | None


@dataclass
class SpecialParameter:
    glob: str | None = None
    root_dir: str | None = None

    range: List[int] | None = None

    def __iter__(self) -> Iterable[str | int]:
        if self.glob:
            yield from glob.iglob(
                os.path.expanduser(self.glob),
                root_dir=(
                    os.path.expanduser(self.root_dir) if self.root_dir else None
                )
            )
        elif self.range:
            yield from list(range(*self.range))


ParameterDict = Dict[str, ParameterValue]
Parameters = Dict[str, ParameterValue | SpecialParameter | List[ParameterValue]]


def split_variables_and_arguments(param_dict: ParameterDict) -> Tuple[ParameterDict, ParameterDict]:
    """Split parameters into variables and arguments."""
    variables, arguments = {}, {}
    for key, value in param_dict.items():
        if key.startswith('$') or key.startswith('-'):
            arguments[key] = value
        else:
            variables[key] = value
    return variables, arguments


def normalize_parameters(params: Parameters | List[Parameters]) -> Iterable[ParameterDict]:
    """Normalize parameters into a list of simple dictionaries with all combinations."""
    if isinstance(params, dict):
        params = [params]

    for param_set in params:
        # First, process any special parameters
        grid_params = {}
        for key, value in param_set.items():
            if isinstance(value, dict):
                grid_params[key] = list(iter(SpecialParameter(**value)))
            elif isinstance(value, list):
                grid_params[key] = value
            else:
                grid_params[key] = [value]

        # Generate all combinations of grid parameters
        keys = list(grid_params.keys())
        values = [grid_params[k] for k in keys]
        for combination in itertools.product(*values):
            yield dict(zip(keys, combination))

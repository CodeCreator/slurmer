# Slurmer

Slurmer simplifies the process of submitting and managing complex job arrays on Slurm-based HPC clusters.
Define your job configurations in YAML and let Slurmer handle the rest - from parameter sweeps to dependency chaining and job tracking.

## Features

- **YAML Configuration**: Define job parameters and parameters in a readable format
- **Parameter Sweeps**: Easily run experiments across different parameter combinations
- **Auto-skip Running and Completed Jobs**: Don't submit jobs that are already running or completed
- **Job Chaining**: Chain multiple jobs with dependencies
- **Dry Run and Interactive Modes**: Preview commands without submission and generate commands for interactive use

## Installation

```bash
# Clone the repository
git clone https://github.com/CodeCreator/slurmer.git
cd slurmer

# Install the package
pip install -e .
```

## Quick Start

1. Create a `runs.yaml` file with your job configurations:

```yaml
example_grid:
  name: "job-{dataset}-{lr}"
  script: "train.sh"
  params:
    dataset: ["cifar10", "mnist"]
    lr: [0.001, 0.01]
    epochs: 100
    "-b": 128  # batch size as argument to the script!
  slurm:
    gres: "gpu:1"
  completion: "~/results/{dataset}_model_lr{lr}.pth"
```

2. Submit all your jobs by running `slurmer`. This will submit the following jobs:
```bash
dataset="cifar10" lr="0.001" epochs="100" sbatch gres gpu:1 -J job-cifar10-0.001 train.sh -b "128"
dataset="cifar10" lr="0.01" epochs="100" sbatch gres gpu:1 -J job-cifar10-0.01 train.sh -b "128"
dataset="mnist" lr="0.001" epochs="100" sbatch gres gpu:1 -J job-mnist-0.001 train.sh -b "128"
dataset="mnist" lr="0.01" epochs="100" sbatch gres gpu:1 -J job-mnist-0.01 train.sh -b "128"
```
If these jobs have already been scheduled on slurm or if they have finished and created the files in the completion path, they will be skipped.

3. You can also submit only a specific job grids or grid from a different config file:
```bash
# Submit specific job groups in a different config file
slurmer -c other_runs.yaml example_grid

# Preview commands without submitting
slurmer --dry-run
```

## Advanced Features

- Using globs to process file sets
- Numeric range parameters
- Job dependency chaining
- Precondition and completion checks
- Custom job naming
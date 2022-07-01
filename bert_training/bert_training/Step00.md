# Step00 - Environment setup

TODO. 

Our intent is to use UTHPC Rocket resources (GPUs) for BERT pre-training and 
fine-tuning. First step (text pre-processing to be suitable for BERT pre-training)
should be possible without GPUs.

Harry asks: 
  _Is it possible to run all pipelines also on CPU-s only (on small datasets, 
   just to see that they all run through correctly)? If not at the moment 
   then can the code be modified so that 'CPU-s only' execution would be possible?_

We use [conda](https://docs.conda.io/en/latest/miniconda.html) as EstNLTK
more-less requires it (otherwise it would be hard to build all related dependencies).

Initial [./environment.yml](./environment.yml) shows all dependencies (main dependencies
mixed with transitive dependencies) and is not usable across platforms; e.g. on macOS
one could see errors like

```
$ conda env create -f environment.yml
Collecting package metadata (repodata.json): done
Solving environment: failed

ResolvePackageNotFound:
- freetype==2.10.4=h546665d_1
- lxml==4.6.3=py38h292cb97_0
- regex==2021.4.4=py38h294d835_0
...
- m2w64-gcc-libs-core==5.3.0=7
- vc==14.2=h21ff451_1
- jupyter_core==4.7.1=py38haa95532_0
```

We need 

* `.yml` file with main dependencies only (rest will be solved by conda)
* In schemes of `major.minor.micro` it seems safe to fix `major.minor` part 
  and keep `micro` free (unless we have very specific needs for some packages)
* Separate development & testing dependencies, at least on comments level
  (e.g. what packages are needed for testing only, which ones for development only)

The initial take on it is at [./medbert.yml](medbert.yml) - this needs additionally:

* testing locally and in the UTHPC environment, with notebooks, tests and real pipeline
* upgrading libraries where possible
* clean-up of remarks/notes once working environment is in place


## Conda environment setup in UTHPC

```
$ ssh <username>@rocket.hpc.ut.ee
$ cd path/to/your/bert-project
$ git clone https://gitlab.cs.ut.ee/health-informatics/medbert.git
```

Load module with `conda` so that you can create our medbert environment:

```
$ module list
No modules loaded

$ module load miniconda3/4.8.2
$ module list

Currently Loaded Modules:
  1) miniconda3/4.8.2

$ conda --version
conda 4.8.2
```

Create the environment:

```
$ conda env create -f medbert.yml
```

To check environment content use:

```
$ conda list --name=medbert
# packages in environment at /gpfs/space/home/<username>/.conda/envs/medbert:
#
# Name                    Version                   Build  Channel
_libgcc_mutex             0.1                 conda_forge    conda-forge
...
estnltk                   1.6.8b0                     3.8    estnltk
...
pytorch                   1.9.0           py3.8_cuda11.1_cudnn8.0.5_0    pytorch
...
zlib                      1.2.11            h516909a_1010    conda-forge
zstd                      1.5.0                ha95c52a_0    conda-forge
```

Check that tests pass:

```
# Loads SLURM module, activates conda environment and executes 'python -m unittest discover -vvv':
$ sbatch tests/sbatch_quick_tests.sh
Submitted batch job <job-id>

$ cat slurm-<job-id>.out 
```

or use wrapper-script that will follow (`tail -f`) created SLURM log-file automatically:

```
$ bash tests/run_quick_tests.sh
```


## Conda environment update in UTHPC

If you have existing conda environment, then apply its updates so:

```
$ ssh <username>@rocket.hpc.ut.ee
[username@rocket]$ cd path/to/your/bert-project/medbert
[username@rocket medbert]$ module load miniconda3/4.8.2
[username@rocket medbert]$ conda env update -f medbert.yml
Collecting package metadata (repodata.json): done
Solving environment: \ 
...

```

## Warning: 'The environment is inconsistent, please check the package plan carefully'

Updating might result in warnings like:

```
The environment is inconsistent, please check the package plan carefully
The following packages are causing the inconsistency:

- pytorch/linux-64::pytorch==1.9.0=py3.8_cuda11.1_cudnn8.0.5_0
- conda-forge/linux-64::cudatoolkit==11.1.1=h6406543_8

done
```

Does it matter? How to fix?

There are various suggestions: 
https://stackoverflow.com/questions/55527354/the-environment-is-inconsistent-please-check-the-package-plan-carefully

Which might or might not to work. However, the last resort is to re-create the environment fully:

```
conda remove --name medbert --all
conda env create -f medbert.yml
```

But after `conda env update -f medbert.yml` the message can still show up :(

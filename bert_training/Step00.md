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

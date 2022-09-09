# B.NanoAmP
Bacterial Nanopore Assembly Pipeline

GUI for building assembly pipelines for Nanopore reads.

[Filtlong](https://github.com/rrwick/Filtlong) filtering

plus assembly choices:

* [Flye](https://github.com/fenderglass/Flye) ( + [Racon](https://github.com/isovic/racon))
* [Raven](https://github.com/lbcb-sci/raven)
* [Miniasm](https://github.com/lh3/miniasm) + [Minipolish](https://github.com/rrwick/Minipolish)

plus [Medaka](https://github.com/nanoporetech/medaka) consensus.


# Installation

### Python

* install python 3.10 and the newest pip version
* install the requirements `pip install -r requirements.txt`

<sub>The whole setup is only tested on python 3.9 and 3.10. Older versions might work, but might also produce unexpected failures. Feel free to open issues with older python versions.</sub>

### Conda

To build the possible pipelines, all the tools need to be available through conda environments, as they are not bundled.
To do so you need at least a conda version installed (e.g. [miniconda](https://docs.conda.io/en/latest/miniconda.html)) where three options are supported concerning the conda installation:

1. Install conda to one of the default paths (where the GUI can detect conda on startup)
```
~/miniconda3/
~/anaconda3/
/opt/miniconda3/
/opt/anaconda3/
```
2. Install conda to a custom location and provide the path to the binary on startup (less recommended, as this needs to be done on each single startup)
3. Install conda and add it to PATH during installation (not recommended, as this additionally performs `conda init` which adds overhead) 


The required tool setup can than be done automatically or manually:
* Let the GUI handle the tool installation automatically on startup (which might take some time) (recommended)
* Install the toolchains manually (using the package lists given in [B.NanoAmP/ressources](https://github.com/simanjo/B.NanoAmP/tree/main/ressources))
or just have the tools in question installed in any conda environments (where they should be autodetected)



### start GUI

`python main.py`

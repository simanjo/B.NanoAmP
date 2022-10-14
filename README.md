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

B.NanoAmP generally requires only a working Python 3.x distribution and a suitable conda version to handle the tool setup.
The used tools are only available on Linux and MacOS but due to a lack of hardware, the test were only performed using debian-based Linux distributions.
While the MacOS setup should be similar, you might run into unknown problems there and feedback is highly appreciated.

As of Windows 11, [WSL2 supports GUI-based Linux applications](https://learn.microsoft.com/en-us/shows/tabs-vs-spaces/wsl-2-run-linux-gui-apps) as well, and B.NanoAmP seems to run in WSL as well (not reasoning about efficiency there), while there occur infrequent bugs in the GUI (keystrokes are sometimes not registered and mouse clicks behave unexpected). A restart of the application should however fix them. 
So generally it is supposed to run under WSL2 but be aware that there might be dragons. (For more details see the last section)

## Linux

Download the latest stable version [v1.1.1](https://github.com/simanjo/B.NanoAmP/archive/refs/tags/v1.1.1.tar.gz)

### Python

* install python 3.10 and the newest pip version
* install the requirements `pip install -r requirements.txt`

<sub>The whole setup is only tested on python 3.8, 3.9 and 3.10. Older versions might work, but might also produce unexpected failures. Feel free to open issues with older python versions.</sub>

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

## Windows using WSL2

Following the instructions [install WSL2](https://learn.microsoft.com/en-us/windows/wsl/tutorials/gui-apps). Note that you need at least Windows 11 Build 22000 for this to work.
The standard ubuntu distribution should already contain the most relevant packages besides conda.
The remaining installation follows the instructions for Linux above.

In case of problems or unexptected behaviour with WSL feel free to open an issue.

# Usage

## Conda Environment Setup
On startup, B.NanoAmP tries to detect a conda version (see the above instructions for conda installation for details.) and if none is found, the following message is displayed, where you can manually select a path to a conda binary.
![Initial view, when no conda installation was detected](https://user-images.githubusercontent.com/82642377/195156035-f99a9fb9-08ca-489f-9ab8-a38216dd449c.png)

If conda was detected, B.NanoAmP uses that conda binary to check for environments on your computer, trying to find all the required pipeline tools. If some or all of them are missing, the below message is displayed, providing the opportunity to have the tools installed in separate conda environments. Note that this might take some time.

![View, when the required pipeline tools are not detected](https://user-images.githubusercontent.com/82642377/195156506-560ce1bb-2ea4-451a-85e9-24767e150441.png)

## Overview of the GUI elements

If you have the necessary conda setup, B.NanoAmP basically consists only of the following view (plus some additional message popups). The whole pipeline setup and execution is done here.

![Overview of the GUI elements](https://user-images.githubusercontent.com/82642377/195156695-3a07d1f4-f936-434c-baaa-1c6fe39be4b9.png)

1. First you need to select the directory, containing the fastq files to assemble. If there are multiple subdirectories containing fastq files, each of the directories is treated as seperate assembly task (think e.g. of different barcodes). Compressed files (tar.gz or .gz) can also be used as input files, and evene a mixture of compressed and uncompressed files can be handled (though it is not advisable to do so).
2. Then you can set some basic configuration items, with the following effect:
   * skip assembly in unclassified folder: skips any folder named "unclassified", which is a commonly found folder resulting from guppy basecalling and usually contains reads that can not be attributed to a barcode
   * keep intermediate results: if checked, the outputs of each of the pipeline steps are kept and not deleted (which is the default setting to keep your directory clean)
   * Threads: determines the amount of threads to use in those tools that allow for such a setting
   * Genome Size: specifies the expected size of your Genome in Megabases
   * Coverage: specifies the desired coverage
   * min_len: setting for filtlong, determining the minimal length of reads to keep
3. Then you can select the assemblers to use, where racon polishing can be included optionally in case of using Flye. More than one assembler can be selected, resulting on several assembly tasks performed one after the other.
4. Finally you have to select the medaka model to use (for mandatory medaka polishing after the assembly). You can either select the relevant parameters and an appropriate model is guessed fitting best, or you can select the model manually from a list of available models.
5. After all the choices are made, you can start the execution of the pipeline, with some additional information about the pipeline progress being displayed in the box below.
   



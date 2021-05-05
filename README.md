# Purpose
This workflow was build and designed to run on the OSG via the Pegasus workflow manager to map the 145mm solid angle corretion parameter surface for gamma-gamma angular correlations (GGAC) in the GRIFFIN HPGe array.

# Usage
## Running the Workflow
To run the workflow use:
```
./ggac_surface.py
```
in the parent workflow directory. Pegasus will look for all datafiles in the `inputs` directory and the exectuables in the `bin` directory.

## Data Files
The workflow requires three data files to properly simulate the GGAC's
1. z{#}.a{#}
2. z{#+1}.a{#}
3. Multipole_z{#+1}.a{#}
where the name of the file dictates the isotope of interest. The creation of these files is automated and handled by the `make_input_files.sh` script.

## Executable functions
All of the exectubale functions run on the exectution nodes during the workflow are in teh `bin` directory.

# Output
Pegasus handles all of the file transfers and data validation and transfers the output files to the remote server specified in the site catalog. Only the sorted files are transferred to the output site and done so in tarballs. It is up to the user to unpack the tarballs on the remote site.

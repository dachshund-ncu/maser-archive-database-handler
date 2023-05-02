# Maser Archive database handler
## Introduction
Set of classes and functions for handling:
- maser database:
    - creating database
    - adding records
    - reading records
    - updating records
- maser archive:
    - copying .fits files

## Requirements
- astropy
- fitsio
- numpy
- pandas

### Installation
```bash
sudo apt install libbz2-dev
sudo apt install gfortran
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```


## Archive working flowchart
```mermaid
flowchart TD

archive_main[Archive main directory] --> sources
sources --> source_1[Source 1] --> m_band[m_band directory] --> fits_files[.fits files]
sources --> source_2[Source 2] --> m_band2[m_band directory] --> fits_files2[.fits files]
sources --> source_3[...]

archive_main --> maser_observations_db[(Maser observations database)]

new_fits_files[New .fits files] --> scripts_for_database((Python scripts for database handling))

scripts_for_database{{Database handler}}

maser_observations_db -- read destination --> scripts_for_database
scripts_for_database -- copy .fits files --> m_band2
scripts_for_database -- update database --> maser_observations_db

subgraph files_place[Archive]
archive_main
sources
source_1
source_2
source_3
m_band
m_band2
fits_files
fits_files2
maser_observations_db
end
```
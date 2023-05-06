__doc__ = "These are helper routines for handling database \
            they allow creating completely new database\
            based on the archive and 6ghz_list.txt file.\
            To create new database you should use:\
            construct_database(os.path.join(DE_CAT, \"6ghz_list.txt\"), archve_dir, output_file"
'''
This file is a part of the
maser-database-handler repository
https://github.com/dachshund-ncu/maser-database-handler
author: Michał Durjasz
md@astro.umk.pl
'''

# imports
import os
import pandas as pd
import glob
import numpy as np
import fitsio as fits
from astropy.time import Time
from db_handler import sources_database

# global: catalogue of helper_routines.py
DE_CAT = os.path.dirname(__file__)

def make_ra_str(ra: str) -> str:
    return ra[0:2] + 'h' + ra[2:4] + 'm' + ra[4:] + 's'
def make_dec_str(dec: str) -> str:
    if float(dec) > 0:
        return dec[0:2] + 'd' + dec[2:4] + 'm' + dec[4:] + 's'
    else:
        return dec[0:3] + 'd' + dec[3:5] + 'm' + dec[5:] + 's'

def pop_flagged_sources(filenames:list, source_dir: str) -> list:
    '''
    Removes flagged files from list
    '''
    if os.path.exists(os.path.join(source_dir, 'flagged_obs.dat')):
        # get flagged files
        flagged_filenames = np.loadtxt(os.path.join(source_dir, 'flagged_obs.dat'), dtype=str, unpack=True, usecols=(0), ndmin=1)
    else:
        return filenames
    # get only basenames
    flagged_filenames = [os.path.basename(fle) for fle in flagged_filenames]
    # pop flaggred files
    for i in range(len(filenames)-1, -1, -1):
        basename = os.path.basename(filenames[i])
        if basename in flagged_filenames or basename.endswith('noedt.fits'):
            filenames.pop(i)
    return filenames

def get_min_max_epoch_files(filenames):
    '''
    return the filenames with min and max epoch
    '''
    # get filenames
    filenames_base = [os.path.basename(f) for f in filenames]
    # extract epochs from the filenames
    epochs = [fle.split("_")[1].split('.')[0] for fle in filenames_base]
    epochs2 = [e[:5] + '.' + e[5:] for e in epochs]
    epochs3 = np.asarray(list(map(float, epochs2)))
    # sort both arrays 
    flist = [x for _,x in sorted(zip(epochs3, filenames))]
    return flist[0], flist[-1]

def get_isotimes_from_files(epochs: tuple) -> str:
    '''
    returns isotimes from files
    '''
    headers = [fits.FITS(e)[1].read_header() for e in epochs]
    return [header["DATE-OBS"] for header in headers]

def get_vlsr_from_file(filename: str) -> float:
    '''
    Returns the V_lsr parameter from fits file
    '''
    return fits.FITS(filename)[1].read_header()["VSYS"]

def get_cadence_from_isotimes(start: str, stop:str, number: int) -> float:
    '''
    Returns the cadence of the observations, based on the given arguments
    '''

    # get time delta
    times = [Time(data, format='isot', scale='utc') for data in [start, stop]]
    dt = times[1] - times[0]
    delta = dt.value
    # return monthly cadence (assuming month == 30 days)
    if delta == 0.0:
        return 0.0
    else:
        return round(number / delta * 30,3)

def get_short_names(filenames: list) -> str:
    '''
    Returns the string with short filenames imposed
    '''
    basenames = [os.path.basename(f) for f in filenames]
    short_names = [base.split('_')[0] for base in basenames]
    unique_names = np.unique(short_names)
    return " ".join(unique_names)

def get_all_fits_filenames(directory):
    return glob.glob(os.path.join(directory, '*.fits'))

def get_source_params(sourcename: str, archive_dir:str):
    '''
    gets the source parameters
    '''
    # load files
    filenames = get_all_fits_filenames(os.path.join(archive_dir, sourcename, 'm_band'))
    if len(filenames) < 1:
        return " ", 0.0, "---", "---", 0, 0.0
    filenames = pop_flagged_sources(filenames, os.path.join(archive_dir, sourcename, 'm_band'))

    # get the first and last epochs
    epochs = get_min_max_epoch_files(filenames)
    min_time, max_time = get_isotimes_from_files(epochs)
    cadence = get_cadence_from_isotimes(min_time, max_time, len(filenames))
    v_lsr = get_vlsr_from_file(epochs[0])
    short_name = os.path.basename(epochs[1]).split('_')[0]
    return short_name, v_lsr, min_time, max_time, len(filenames), cadence


def get_sourcenames(archive_dir: str) -> list:
    '''
    Returns list with the source names
    '''
    sources = []
    for root, dir, file in os.walk(archive_dir, topdown=True):
        for directory in dir:
            if not directory.endswith('_band'):
                sources.append(os.path.basename(directory))
    return sources


# helper scripts
def get_all_sources_parameters(parameters_file: str, archive_dir: str):
    '''
    return all sources parameters
    '''
    context_dict_l = []
    sources_df_parameters = pd.read_csv(parameters_file, delimiter=" ", header=None, dtype=str)
    sources = get_sourcenames(archive_dir)
    for row in sources_df_parameters.iterrows():
        if row[1][0] in sources:
            name = row[1][0]
            ra = make_ra_str(row[1][2])
            dec = make_dec_str(row[1][3])
            short_name, v_lsr, first_obs, last_obs, number, number_cadence = get_source_params(name, archive_dir)

            context_dict_l.append({'name': name, 'ra': ra, 'dec': dec, 'short_name': short_name, 'v_lsr': v_lsr, 'obs_first': first_obs, 'obs_latest': last_obs, 'obs_number': number, 'mean_cadence_per_month': number_cadence})
        
    return context_dict_l


def construct_database(parameters_file: str, archve_dir: str, output_file: str):
    '''
    Makes a new database file, based on the archive provided in directory
    '''
    # get the dictionary for database
    context_dict_list = get_all_sources_parameters(parameters_file, archve_dir)
    # create database
    database = sources_database(output_file)
    # add table
    database.create_table('sources')
    # add sources to the table
    for d in context_dict_list:
        database.add_source(tuple(d.values()))

if __name__ == '__main__':
    construct_database('/home/michu/projects/maser-archive-dashboard/database_handler/6ghz_list.txt', '/home/michu/projects/maser-archive-dashboard/archive', 'maser_database.db')
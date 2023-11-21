__doc__ = "This is file for handling the maser archives: copying\
            and updating the database"

'''
This file is a part of the
maser-database-handler repository
https://github.com/dachshund-ncu/maser-database-handler
author: Michał Durjasz
md@astro.umk.pl
'''
import os, shutil, numpy as np
from . import db_handler
from .database_creator import get_source_params, make_ra_str, make_dec_str
import pandas as pd
import streamlit as st

DE_CAT = os.path.dirname(__file__)
ARCHIVE_DIR = '/home/michu/Drop/Dropbox/metvar_archive/fits_sources'
DATABASE_FILE = '/home/michu/Drop/Dropbox/metvar_archive/maser_archive_db.db'

def create_new_source_in_archive(full_source_name: str, short_name: str, db_file: db_handler.sources_database):
    '''
    Creates new source in the database
    '''
    sources_params = pd.read_csv(os.path.join(DE_CAT, '6ghz_list.txt'), header=None, dtype=str, delimiter=" ")
    data_slice = sources_params[sources_params.iloc[:,0] == full_source_name]
    data_tuple = (full_source_name, make_ra_str(data_slice[2].values[0]), make_dec_str(data_slice[3].values[0]), short_name, float(data_slice[4].values[0]), '---', '---', 0, 0.0)
    add_source_to_database(data_tuple, db_file)
    os.mkdir(os.path.join(ARCHIVE_DIR, full_source_name))
    os.mkdir(os.path.join(ARCHIVE_DIR, full_source_name, 'm_band'))
    return data_tuple
def add_source_to_database(source_tuple: tuple, db_file: db_handler.sources_database):
    '''
    Adds source to the database file
    '''
    print("Adding database entry: ", source_tuple)
    db_file.add_source(source_tuple)

def split_to_sources(fits_files: str) -> dict:
    '''
    Splits the fits_files table into sources
    Arguments:
    | fits_files: a table with all of the data-reducted fits files
    Returns:
    | splitted_fits_files - a table with fits_files, but groupped by source
    | sources_short_names - corresponding table with sourcenames
    '''
    basenames = [os.path.basename(f_file) for f_file in fits_files]
    # get unique filenames
    sources_short_names = np.unique([f_file.split('_')[0] for f_file in basenames])
    # dictionary
    context_dict = {}
    for src_name in sources_short_names:
        context_dict[src_name] = []
    # iterate through whole dataset to 
    for f_file in fits_files:
        s_name = os.path.basename(f_file).split('_')[0]
        context_dict[s_name].append(f_file)
    return context_dict

def split_to_sources_st(fits_files) -> dict:
    '''
    Splits the fits_files table into sources - accepts streamlit IO
    Arguments:
    | fits_files: a table with all of the data-reducted fits files (Streamlit IO)
    Returns:
    | splitted_fits_files - a dict with fits_files, but groupped by source
    '''
    basenames = [os.path.basename(f_file.name) for f_file in fits_files]
    # get unique filenames
    sources_short_names = np.unique([f_file.split('_')[0] for f_file in basenames])
    # dictionary
    context_dict = {}
    for src_name in sources_short_names:
        context_dict[src_name] = []
    # iterate through whole dataset to 
    for f_file in fits_files:
        s_name = os.path.basename(f_file.name).split('_')[0]
        context_dict[s_name].append(f_file)
    return context_dict

def copy_fits_files_st(fits_filenames, destination_dir: str):
    '''
    Copies the fits files given in the argument to the destination dir
    '''
    for f_file in fits_filenames:
        with open(os.path.join(destination_dir, f_file.name), "wb") as f:
            f.write(f_file.getbuffer())


def copy_fits_files(fits_filenames: str, destination_dir: str):
    '''
    Copies the fits files given in the argument to the destination dir
    '''
    for f_file in fits_filenames:
        # print(f"cp {f_file} {destination_dir}")
        shutil.copyfile(f_file, os.path.join(destination_dir, os.path.basename(f_file)))

def update_database(database: db_handler.sources_database, source_record: tuple, destination_dir: str):
    '''
    Updates the redcord in the database, based on the destination_dir contents
    Arguments:
    | database:        object of the database handler
    | source_record:   tuple with the source record to update
    | destination_dir: location of the source data in the archive
    '''
    # get mutable recort (as tuples are not mutable)
    mutable_record = list(source_record)

    # get params tuple
    params = get_source_params(source_record[1], os.path.dirname(os.path.dirname(destination_dir)))
    # print(len(tuple(mutable_record)))
    mutable_record[4:] = list(params)
    # print(len(tuple(mutable_record)))
    database.update_source(tuple(mutable_record[1:]))



def copy_files_to_database(fits_files: str, archive_dir: str, database_file: str) -> bool:
    '''
    This routine manages copying the .fits files to the database
    Arguments:
    | fits_files:     fits files to copy to the archive (best practice is to use absolute paths)
    | archive_dir:    directory of the maser archive (absolute path)
    | database_file:  file with the database
    '''
    database = db_handler.sources_database(database_file)
    source_dictionary = split_to_sources(fits_files)
    for short_name in source_dictionary.keys():
        if short_name == 'g32p74':
            continue
        # get source params
        source_tuple = database.get_from_short_name(short_name)

        # exception handle: short name not present in database
        if source_tuple is None:

            # choose source manually
            print(f"For {short_name} we did not find a proper source in database. Please provide it by yourself:")
            source_full_name = input("---> ")
            source_tuple = database.get_source(source_full_name)

            # if source is not present in database - get new from text file
            if source_tuple is None:
                # create new source in the database
                source_tuple = create_new_source_in_archive(source_full_name, short_name, database)
        
        if source_tuple is not None:
            # destination directory
            destination_dir = os.path.join(archive_dir, source_tuple[1], 'm_band')
            # copy the files
            copy_fits_files(source_dictionary[short_name], destination_dir)
            # update db
            update_database(database, source_tuple, destination_dir)
        
        else:
            print("Failed to copy files to the database")

def move_files_to_database(files, archive_dir: str, database: db_handler.sources_database) -> list:
    """
    This routine manages copying the .fits files to the database
    Arguments:
    | fits_files:     files (streamlit IO) to copy
    | archive_dir:    directory of the maser archive (absolute path)
    | database_file:  file with the database
    """
    # database = db_handler.sources_database(database_file)
    source_dictionary = split_to_sources_st(files)
    sources = []
    for short_name in source_dictionary.keys():
        if short_name == 'g32p74':
            continue
        # get source params
        source_tuple = database.get_from_short_name(short_name.strip())
        # exception handle: short name not present in database
        if source_tuple is not None:
            # destination directory
            destination_dir = os.path.join(archive_dir, source_tuple[1], 'm_band')
            # copy the files
            copy_fits_files_st(source_dictionary[short_name], destination_dir)
            # update db
            update_database(database, source_tuple, destination_dir)
            # add to sources
            sources.append(source_tuple[1])
            st.write(f"Added {len(source_dictionary[short_name])} files to {source_tuple[1]}")
        else:
            # choose source manually
            # print(f"For {short_name} I did not find a proper source in database!")
            st.write(f"For {short_name} I did not find a proper source in database!")
    return sources

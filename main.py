from __future__ import print_function

import os.path
import re
import os
import shutil
import sqlite3
import io
import requests
import builtins
import time
import json
import argparse
import pathlib
import webbrowser
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from pprint import pprint
from pathlib import Path
from rich.console import Console
from rich.tree import Tree
from rich.live import Live
from dateutil import parser
from PyPDF2 import PdfReader
from threading import Lock

# Command line arguments
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--clean', action="store_true", help="Force a clean redownload of songs, clear local cache. This does NOT clean the Google Drive folder, do that manually if needed (it shouldn't be needed).") 
arg_parser.add_argument('--skipquery', action="store_true", help="Used for inner dev loop. Stores the file metadata of the drive so we don't have to requery each song. But usually you want to requery.")
arg_parser.add_argument('--skipupload', action="store_true", help="Used for inner dev loop. Skips uploading the databases files at the end.")
arg_parser.add_argument('--verbose', action="store_true", help="Spit out extra info") 
arg_parser.add_argument('--dedupe', action="store_true", help="For use when a part folder accidentally ends up with multiple copies of the same file. Shouldn't happen.") 
args = arg_parser.parse_args()

# Relevant Google Drive folders - TODO read this from a .ini file instead
WEEKLY_AGENDA_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"
MEMORIZATION_LIST_ID = "1lYz54_jarIxfqZu0vVebfcRIBykGsikdSs3QNhlecU0" # TODO - this just links to mp3s, not song folders, maybe remove
DEST_MUSIC_FOLDER = "1h-T2mnFrr0VpafBDJ3nv3vvO_xLGir9t" # The folder where the MobileSheets database and PDFs will end up
SRC_MUSIC_FOLDER = "12y2cjGE7GE3MTJ8QtNs3_Z5L30o5Ql6D" # The LTBB folder containing all the sheet music. Currently organized in folders like 'A-C', 'D-F', etc
SEASONAL_SONGS = "1M7sLr9wwvHJIfKGijRTSjC5ae1CODzbY" # Some subfolders that contain additional songs not in the alphabetic folders
DRIVE_ID = "0AIscw8ywGnshUk9PVA" # Quirk of using a Shared Drive, we sometimes need this
IGNORE_FOLDERS = ['1. Member Drafts', '2. Seasonal Songs', '3. Warm-ups', '4. 3rd Rail Drumline', '5. Resources', '6. Recordings']
MAX_SONGS = 99999

# Instrumentation - this could also possibly move to a .ini folder
INSTRUMENTS = {
    'Score': ['Score'],
    'Tuba': ['Tuba', 'Sousaphone', 'Sousa', 'Low Brass', 'Basses', 'Bass (Treble Clef)', 'Bass_Line'],
    'Horn': ['Horn in F', 'F Horn', 'Mellophone', 'Horns F'],
    'Percussion': ['Percussion', 'Drum', 'Snareline', 'Perc', 'BassDr', 'Snare', 'Congo', 'Toms', 'Quads', 'Cymbal', 'Glockenspiel'],
    'Clarinet': ['Clarinet', 'Bb Sopranos'],
    'Soprano Sax': ['Soprano'],
    'Alto Sax': ['Alto'],
    'Tenor Sax': ['Tenor'],
    'Bari Sax': ['Bari'],
    'Bass Sax': ['Bass Sax', 'Bass Saxophone'],
    'Trumpet': ['Trumpet', 'Flugelhorn', 'Trmp', 'Trumplet'],
    'Trombone': ['Trombone', 'Tbn', 'Trmb', 'Bone', 'Low Brass'],
    'Eb Horn' : ['Eb Horn', 'Horn in Eb'],
    'Flute' : ['Flute', 'C Woodwind'],
    'Euph' : ['Euphonium', 'Euph']
}
# Does your instrument not have a part for this song? Use a backup!
BACKUP_INSTRUMENTS = {
    'Score': [],
    'Tuba': ['Bass Clef Instruments'],
    'Horn': ['F Treble Clef Instruments', 'Horn'], # Horn included here so we don't accidentally pick up a zillion Eb Horns without at least trying things like 'Horn in F' first
    'Percussion': [],
    'Clarinet': ['Bb Treble Clef Instruments'],
    'Soprano Sax': ['Bb Treble Clef Instruments', 'Tenor Sax'],
    'Alto Sax': ['Eb Treble Clef Instruments'],
    'Tenor Sax': ['Bb Treble Clef Instruments'],
    'Bari Sax': ['Eb Treble Clef Instruments'],
    'Bass Sax': ['Bb Treble Clef Instruments', 'Tuba'],
    'Trumpet': ['Bb Treble Clef Instruments'],
    'Trombone': ['Bass Clef Instruments'],
    'Eb Horn' : ['Eb Treble Clef Instruments', 'Bari Sax'],
    'Flute' : ['C Treble Clef Instruments'],
    'Euph' : ['Eb Treble Clef Instruments', 'Tenor Sax']
}
INSTRUMENT_LOOKUP = {}
for main in INSTRUMENTS:
    for sub in INSTRUMENTS[main]:
        INSTRUMENT_LOOKUP[sub.lower()] = main

# Globals for Drive access
def get_creds():
    creds = None
    SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/documents.readonly"]
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not os.path.exists("credentials.json"):
        print("You need to generate a credentials.json from Google in order for this script to work!")
        print("This means setting up a Google Cloud project (sorry you cannot use mine for security reasons)")
        print("Instructions: https://developers.google.com/workspace/drive/api/quickstart/python")
        exit()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds
creds = get_creds()
docs = build("docs", "v1", credentials=creds)
drive = build("drive", "v3", credentials=creds)

# Globals for logging
def my_log_print(*args, print_to_std_out=True, save_to_file=True, live=None, **kwargs):
    s = log_indent + stringify_print(*args, **kwargs).rstrip('\n')
    if live:
        live.update(s)
    elif print_to_std_out:
        console.print(s, highlight=False)
    if save_to_file:
        file_console.print(s, highlight=False)
log_indent = ''
error_log = []
console = Console(record=True)
file_console = Console(record=True, file=io.StringIO())
og_print = builtins.print
builtins.print = my_log_print

###############################################
######## Main Execution Starts Here!!! ########
###############################################
def main():
    # Clean download cache
    if args.clean:
        if os.path.exists('cache'):
            shutil.rmtree('cache')

    # Assemble song list
    songs = []
    cache = load_dict('cache/cache.json')
    push_log_section('[cyan]Querying LTBB Drive')
    if args.skipquery and cache and 'songs' in cache and 'setlists' in cache:
        # For inner dev loop, we can skip the query of the google drive folders and docs
        print('[cyan]Loading songs from cached file!')
        songs = cache['songs']
        setlists = cache['setlists']
    else:
        # Query the LTBB main Drive for one gazillion PDFs
        songs = query_tree([SRC_MUSIC_FOLDER, SEASONAL_SONGS])
        remove_duplicate_dicts(songs)
        # Read the rehearsal schedule, modify songs if needed
        # TODO - memorization list actually does not link to any sheet music
        setlists = query_setlist_docs({"Rehearsal": WEEKLY_AGENDA_ID}, songs)
    pop_log_section()
    print("[cyan]Done querying!")
    time.sleep(1)

    # Cache the result of the queries for inner dev loop
    os.makedirs('cache', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    save_dict('cache/cache.json', {'songs':songs, 'setlists':setlists})

    # Figure out the instrumentation from each songs' filenames
    print()
    push_log_section('[cyan]Assembling part information')
    for song in songs:
        push_log_section("Instrumentation for [green]" + song['name'])
        assemble_song_parts(song)
        pop_log_section()
    pop_log_section()
    print("[cyan]Part information assembled!")

    # Warn about files missing instruments
    partless_files = find_partless_files(songs)
    if len(partless_files) > 0:
        warn('[yellow]Some files were not associated with any instrument (they might be Conductor Scores):', silent=True)
        for file in partless_files:
            warn(    '[yellow]    ' + file['name'], silent=True)
    print("[cyan]See part information at [green]cache/songs_with_parts.json")
    save_dict('cache/songs_with_parts.json', songs)
    time.sleep(1)

    # Rename all files of the form 'Instrument - SongTitle.pdf" into "SongTitle - Instrument.pdf" because then they'll be alphabetical
    get_song_preferred_names(songs)

    # Find destination part Drive folder IDs and existing files
    print()
    push_log_section("[cyan]Querying destination part folders...")
    part_folders = {}
    for part in INSTRUMENTS:
        push_log_section("Finding part folder [magenta]" + part)
        folder = get_or_create_folder(part, DEST_MUSIC_FOLDER)
        folder['files'] = list_pdfs_in_folder(folder['id'])
        part_folders[part] = folder
        print("Found " + str(len(folder['files'])) + " existing PDFs")
        pop_log_section()
    pop_log_section()

    # Dedupe files in the Google Drive (shouldn't need to happen)
    if args.dedupe:
        push_log_section("[cyan]Deduping files, because somehow Geoffrey ended up getting multiple copies of the same PDF into a part folder.")
        for part in part_folders:
            dedupe_files(part_folders[part])
        pop_log_section()

    # Copy files from Src drive folder to Destination drive folder 
    # Skips if the Src song is not newer than the Dest song
    print()
    push_log_section('[cyan]Copying songs from source folders into destination part folders...')
    copy_songlist_into_drive(songs, part_folders)
    pop_log_section()
    print('[cyan]Songs copied into Drive!')
    time.sleep(1)
        
    # Update MobileSheets Database and upload
    print()
    push_log_section("[cyan]Updating databases...")
    if not args.skipupload:
        update_database(songs, setlists, part_folders)
    pop_log_section()
    print("[cyan]Database updated!")
    time.sleep(1)

    # Detect instruments that are missing parts for a song in the setlist
    for setlist in setlists:
        missing_parts = []
        for song_idx in setlist['song_index']:
            song = songs[song_idx]
            missing_part = {'name':song['name'], 'parts':[]}
            for part_key in part_folders:
                if part_key not in song['parts'] and part_key not in ['Flute', 'Score', 'Percussion']:
                    missing_part['parts'].append(part_key)
            if missing_part['parts']:
                missing_parts.append(missing_part)
        if missing_parts:
            warn(f'[yellow]Setlist [cyan]{setlist['name']}[/cyan] is missing parts in the following songs:', silent=True)
            warn('[yellow](Geoffrey can help get this sorted out)', silent=True)
            for missing_part in missing_parts:
                error(f'    [green]{missing_part['name']}[/green]: ' + str(missing_part['parts']), silent=True)

    # Print errors
    if error_log:
        print()
        print("[cyan]The following warnings/errors occured:")
        for error in error_log:
            print(error)

def get_song_preferred_names(songs):
    possible_instruments = [key.lower().replace(' ', '_') for key in INSTRUMENT_LOOKUP]
    for key in BACKUP_INSTRUMENTS:
        possible_instruments.extend([word.lower().replace(' ', '_') for word in BACKUP_INSTRUMENTS[key]])
    for song in songs:
        for file in song['files']:
            # Figure out if the file name starts with an instrument
            file_name_split_sanitized = file['name'][:-4].lower().replace(' ', '_').replace('.','').split('-')
            if len(file_name_split_sanitized) > 1:
                for possible_key in possible_instruments:
                    if possible_key in file_name_split_sanitized[0]:
                        file_name_split = file['name'][:-4].split('-')
                        file['preferred_name'] = "".join(file_name_split[1:]).strip() + ' - ' + file_name_split[0].strip()
                        if args.verbose:
                            print("    Turned [green]" + file['name'][:-4] + "[/green] into [green]" + file['preferred_name'] + "[/green] for alphabet reasons", print_to_std_out=False)
                        break
            if 'preferred_name' not in file:
                file['preferred_name'] = file['name'][:-4]

def warn(message, silent=False):
    message = '[yellow]WARNING: [/yellow]' + message
    if not silent:
        print(message)
    error_log.append(message)

def error(message, silent=False, crash=False):
    message = '[red]ERROR: [/red]' + message
    if crash or not silent:
        print(message)
    error_log.append(message)
    if crash:
        print(5 / 0)

def push_log_section(section_name, live=None, save_to_file=True):
    print(section_name, live=live, save_to_file=save_to_file)
    global log_indent
    log_indent += '    '

def pop_log_section():
    global log_indent
    log_indent = log_indent[:-4]

def stringify_print(*args, highlight=False, **kwargs):
    buf = io.StringIO()
    # Mimic print() formatting (sep, end, etc.)
    og_print(*args, file=buf, **{k: v for k, v in kwargs.items() if k != "file"})
    return buf.getvalue()

# Used for caching queries (internal dev loop only) and saving log data
def save_dict(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_dict(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

MAX_RETRIES = 5
BASE_DELAY = 1  # seconds
def drive_list_with_retry(drive, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return drive.files().list(**kwargs).execute()
        except HttpError as e:
            if e.resp.status == 500:
                delay = BASE_DELAY * (2 ** (attempt - 1))  # exponential backoff
                print(f"Got 500 error, retrying in {delay} seconds… (attempt {attempt})")
                time.sleep(delay)
            else:
                raise  # re-raise other HTTP errors
    # If we get here, all retries failed
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries due to repeated 500 errors")


# Runs a Google Drive files() query and handles large numbers of files. Returns the list of files.
def query_drive_files(query, fields):
    page_token = None
    files = []

    # Need to include nextPageToken because sometimes there are more than 100 files.
    fields = f"nextPageToken, {fields}"

    while True:
        response = drive_list_with_retry(
            drive,
            q=query,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
            pageToken=page_token,
            fields=fields
        )

        # Get the next page of files
        files.extend(response['files'])
        page_token = response.get('nextPageToken')

        if not page_token:
            break
    return files

# Gets all subfolders in a Google Drive folder
def list_folders_in_folder(folder_id):
    return query_drive_files(
        query=f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name)",
    )

# Gets all PDF files in a Google Drive folder
def list_pdfs_in_folder(folder_id):
    files = query_drive_files(
        query=f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false",
        fields="files(id, name, size, createdTime, modifiedTime, parents)"
    )

    files

    # Populate extra metadata we will need
    for file in files:
        file['filehash'] = java_string_hashcode(file['name'])
        dt = parser.isoparse(file['modifiedTime'])
        file['modifiedTime'] = int(dt.timestamp() * 1000)
        dt = parser.isoparse(file['createdTime'])
        file['createdTime'] = int(dt.timestamp() * 1000)
    return files

# Shorter query to tell if a folder contains any PDFs
def folder_contains_pdfs(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1  # we only need to know if at least one exists
    ).execute()
    
    return len(results.get("files", [])) > 0

# Queries a list of top-level folders and assmebles
def query_tree(root_ids):
    # Get top level folders
    top_level_folders = []
    for root_id in root_ids:
        top_level_folders += list_folders_in_folder(root_id)
    top_level_folders.sort(key=lambda folder: folder['name'])
    top_level_folders = [folder for folder in top_level_folders if folder['name'] not in IGNORE_FOLDERS]

    # Get subfolders
    folders = []
    i = 0
    push_log_section("Querying source folders...")
    with Live(log_indent + "Querying...", console=console, refresh_per_second=4) as live:
        for folder in top_level_folders:
            print("Querying folder [green]" + folder['name'], live=live)
            folders += list_folders_in_folder(folder['id'])
            i+=1
            if i >= MAX_SONGS:
                break
        print(f"Finished querying [cyan]{len(folders)}[/cyan] folders!", live=live)
    pop_log_section()
    # Get PDFs
    folders.sort(key=lambda folder: folder['name'])
    i = 0
    push_log_section("Assembling songs from source folders...")
    with Live(log_indent + "Assembling songs...", console=console, refresh_per_second=4) as live:
        for folder in folders:
            print("Assembling song [green]" + folder['name'], live=live)
            folder['files'] = list_pdfs_in_folder(folder['id'])
            i += 1
            if i >= MAX_SONGS:
                break
        print(f"Finished assembling [cyan]{len(folders)}[/cyan] songs including [cyan]{sum([len(folder['files']) for folder in folders])}[/cyan] files!", live=live)
    pop_log_section()
    
    folders = [folder for folder in folders if 'files' in folder and len(folder['files']) > 0]

    return folders

# Finds setlists in a list of docs, and merges any missing songs into the song list
def query_setlist_docs(setlist_docs, songs):
    setlists = []
    for setlist_name in setlist_docs:
        push_log_section("Querying for setlist songs from doc '[cyan]" + setlist_name + "[/cyan]'")
        setlist_doc_id = setlist_docs[setlist_name]
        setlist_songs = scrape_song_list(setlist_doc_id)
        # An index into the song list
        setlist_index = insert_setlist_songs_into_songlist(setlist_songs, songs)
        # Assemble setlist by name
        setlist = {}
        setlist['name'] = setlist_name
        setlist['song_index'] = setlist_index
        setlists.append(setlist)
        pop_log_section()
    return setlists

# Finds PDF files in a song that did not match any instrument
def find_partless_files(songs):
    partless_files = []
    for song in songs:
        seen_files = set()
        for part in song['parts']:
            for file in song['parts'][part]:
                if file['name'] not in seen_files:
                    seen_files.add(file['name'])
        for file in song['files']:
            if file['name'] not in seen_files:
                partless_files.append(file)
    return partless_files

def filename_contains(file_name, test_string):
    return test_string.lower().replace(' ', '_') in file_name.lower().replace(' ', '_').replace('.','')

# Figure out instrumentation from song titles and which files belong to which instrument
def assemble_song_parts(song):
    files = song['files']
    song['parts'] = {}
    for file in files:
        file_name = file['name']
        parts = extract_parts_from_filename(file_name)
        for part in parts:
            if part not in song['parts']:
                song['parts'][part] = []
            song['parts'][part].append(file)
            print("[magenta]" + part + "[/magenta]: [green]" + file['name'])
    
    # If a part doesn't have a file, try a backup
    for part_key in INSTRUMENTS:
        if part_key not in song['parts']:
            found = False
            for backup_part in BACKUP_INSTRUMENTS[part_key]:
                # Directly take the part if it's in there
                if backup_part in song['parts']:
                    song['parts'][part_key] = song['parts'][backup_part]
                    print("[magenta]" + part_key + "[/magenta] using backup part [green]" + str([file['name'] for file in song['parts'][backup_part]]))
                    break
                else:
                    # The backup part might be something werid like "Bb Treble Clef Instruments", so do another filename test
                    for file in files:
                        if filename_contains(file['name'], backup_part):
                            song['parts'][part_key] = [file]
                            print("[magenta]" + part_key + "[/magenta] using backup part [green]" + file['name'])
                            found = True
                            break
                    if found:
                        break
            if part_key not in song['parts'] and part_key not in ['Flute', 'Percussion', 'Score']:
                warn("No part file found for instrument [magenta]" + part_key + "[/magenta] for song [green]" + song['name'])



    for file in song['files']:
        found = False
        for part_key in song['parts']:
            if file in song['parts'][part_key]:
                found = True
                break
        if not found:
            print("[yellow]Instrument not found for file: " + file['name'])


# Function for getting a sanitized instrument/part name out of "MySong123 - __Tenor__123_v4"
def extract_parts_from_filename(file_name):
    if not file_name.endswith('.pdf'):
        return []
    instruments = []
    file_name_sanitized = file_name.lower().replace(' ', '_').replace('.','')
    for possible_instrument in INSTRUMENT_LOOKUP:
        if filename_contains(file_name, possible_instrument) and INSTRUMENT_LOOKUP[possible_instrument] not in instruments:
            instruments.append(INSTRUMENT_LOOKUP[possible_instrument])
    return instruments

# Gets a Google Drive folder name from its ID
def get_folder_name(folder_id):
    return drive.files().get(
        fileId=folder_id,
        fields="id, name",
        supportsAllDrives=True
    ).execute()['name']

def extract_folder_id(url):
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None

# Scrapes a doc (like the Weekly Agenda) and extracts all songs linked
def scrape_song_list(doc_id):
    doc = docs.documents().get(documentId=doc_id).execute()
    content = doc["body"]["content"]

    links = []

    # Find links
    for element in content:
        if "paragraph" not in element:
            continue
        for run in element["paragraph"]["elements"]:
            text = run.get("textRun", {})
            if "textStyle" in text and "link" in text["textStyle"]:
                links.append(text["textStyle"]["link"]["url"])

    links = [link for link in links if bool(re.match(r"^https://drive\.google\.com/drive/.*folders/.*", link))]

    songs = []
    # Get files at Drive links
    for link in links:
        folder_id = extract_folder_id(link)
        folder_name = get_folder_name(folder_id)
        folder = {'id': folder_id, 'name': folder_name, 'files': list_pdfs_in_folder(folder_id)}
        
        if len(folder['files']) > 0:
            print('Found folder in doc: [green]' + folder_name)
            songs.append(folder)
        else:
            print('Found folder in doc: [green]' + folder_name + '[/green] (skipping, no PDFs found)')
    
    return songs

def list_subfolders(parent_folder_id):
    return query_drive_files(
        query =  f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name)"
    )

# Drive Create folder
def create_folder(name, parent_id=None):
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    if parent_id:
        file_metadata["parents"] = [parent_id]

    folder = drive.files().create(
        body=file_metadata,
        fields="id, name, parents",
        supportsAllDrives=True
    ).execute()
    print("Created folder [magenta]" + name, "[/magenta]: " + folder['id'])

    return folder

# Drive Create folder if does not exist
def get_or_create_folder(name, parent_id=None):
    query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents and trashed = false"

    files = query_drive_files(
        query = query,
        fields="files(id, name)",
    )

    if files:
        return files[0]   # already exists

    # otherwise create it
    file = create_folder(name=name, parent_id=parent_id)
    return file


def escape_drive_query(name):
    # escape single quotes by doubling them
    return name.replace("'", "\\'")

# Drive copy file if newer
def sync_file(source_file, dest_folder, existing_file=None, new_name=None, live=None):
    # Get source file metadata
    source_name = new_name or source_file["name"]
    source_modified = source_file["modifiedTime"]

    if existing_file:
        existing_modified = existing_file["modifiedTime"]

        # Compare modified timestamps
        if source_modified > existing_modified:
            print(f"Source file is newer. Replacing '{source_name}'", live=live)
            # Delete the old copy
            # Don't use delete() anymore since that is a permanent operation and requires Drive membership
            # drive.files().delete(fileId=existing["id"], supportsAllDrives=True).execute()
            drive.files().update(
                fileId=existing_file["id"],
                body={"trashed": True},
                supportsAllDrives=True
            ).execute()
        else:
            if args.verbose:
                print(f"Existing file '{source_name}' is up-to-date. Skipping copy.", live=live)
            return existing_file["id"]  # nothing to do

    # Copy the source file into the folder
    new_file_metadata = {"parents": [dest_folder['id']], "name": source_name}
    copied_file = drive.files().copy(fileId=source_file['id'], body=new_file_metadata, fields="id, name", supportsAllDrives=True).execute()
    print(f"Copied '[green]{source_name}[/green]' to folder '[magenta]{dest_folder['name']}[/magenta]'")
    return copied_file["id"]

def get_file_metadata(file_id):
    return drive.files().get(
        fileId=file_id,
        fields="id, name, mimeType, size, createdTime, modifiedTime, md5Checksum, parents",
        supportsAllDrives=True
    ).execute()

# De-dupe files... for debugging when things get messed up
def dedupe_files(folder):
    seen = set()
    for file in folder['files']:
        if file['name'] not in seen:
            seen.add(file['name'])
        else:
            print(f"De-duping '[green]{file['name']}[/green]' with ID {file['id']} in folder '[magenta]{folder['name']}'[/magenta]")
            drive.files().update(
                fileId=file["id"],
                body={"trashed": True},
                supportsAllDrives=True
            ).execute()

# Make copies of files to my Drive
def copy_songlist_into_drive(songs, part_folders):
    with Live(log_indent + "Finding files...", console=console, refresh_per_second=10) as inner_live:
        with Live(log_indent + "Copying...", console=console, refresh_per_second=10) as outer_live:
            up_to_date = 0
            new_files = 0
            updated_files = 0
            for song in songs:
                push_log_section("Copying files for [green]" + song['name'], live=outer_live, save_to_file=args.verbose)
                for part_key in song['parts']:
                    files = song['parts'][part_key]
                    # Some parts have more than one chart (trumpet 1/2), so copy all files
                    for file in files:
                        needs_copy = True
                        existing_dest_file = None
                        for dest_file in part_folders[part_key]['files']:
                            if dest_file['name'] == file['name']:
                                print("Found an existing file for [green]" + file['name'], live=inner_live, save_to_file=args.verbose)
                                existing_dest_file = dest_file
                        if not existing_dest_file:
                            print("No existing file found in destination folder for [green]" + file['name'], live=inner_live, save_to_file=args.verbose)

                        copied = sync_file(
                            source_file=file,
                            dest_folder=part_folders[part_key],
                            existing_file=existing_dest_file,
                            new_name=file['name'],
                            live=inner_live,
                        )
                        if not existing_dest_file:
                            new_files += 1
                        elif copied == existing_dest_file['id']:
                            up_to_date += 1
                        else:
                            updated_files += 1

                pop_log_section()
            print(f"Finished copying all songs!", live=outer_live)
            print(f"[cyan]{new_files}[/cyan] new files. [cyan]{updated_files}[/cyan] changed files. [cyan]{up_to_date}[/cyan] files up to date.", live=inner_live)


# Uploads a file, deleting an existing one if it exists.
def upload_to_drive(local_path, dest_name, parent_folder_id, live=None):
    # Look for existing file with this exact name in this exact folder
    query = (
        f"name = '{dest_name}' "
        f"and '{parent_folder_id}' in parents "
        "and trashed = false"
    )

    files = query_drive_files(
        query = query,
        fields = "files(id, name)",
    )

    # Delete existing file(s) with that name
    for f in files:
        print(f"Deleting old {f['name']} ({f['id']})", live=live)
        # The delete call permanently deletes, which requires Drive membership
        # I am but a lowly Content Manager, so I will move to trash, which is also much safer
        # and I didn't know existed until Google Drive prevented me from doing it via API
        # drive.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
        drive.files().update(
            fileId=f["id"],
            body={"trashed": True},
            supportsAllDrives=True
        ).execute()

    # Upload the new file
    file_metadata = {
        "name": dest_name,
        "parents": [parent_folder_id],
    }

    media = MediaFileUpload(local_path, resumable=True)

    uploaded = drive.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name",
        supportsAllDrives=True
    ).execute()

    print(f"Uploaded {uploaded['name']} ({uploaded['id']})", live=live)
    return uploaded["id"]

def clear_output_folder(live=None):
    folder = 'output'
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)
            if args.verbose:
                print(f"Deleted {path}", live=live)

# Create a .db file from a template, removing the old one if it exists.
def create_database(db_name, live=None):
    db_path = 'output/' + db_name.replace(' ','_').lower() + '.db'
    if os.path.exists(db_path):
        print('Removing old ' + db_path + ' and replacing with a blank fresh library db', live=live)
        os.remove(db_path)
    if args.verbose:
        print('Created ' + db_path, live=live)
    shutil.copy("ltbb_blank.db", db_path)

# Hashcode function, kind of close to the function that MobileSheets uses,
# but I think we're okay if we don't have exactly the same one. We'll find out I guess.
def java_string_hashcode(s: str) -> int:
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h & 0x80000000:
        h = -((~h + 1) & 0xFFFFFFFF)
    return h

# PDF page counter
def get_page_count(path):
    reader = PdfReader(path)
    return len(reader.pages)

# File download
def download_pdf_for_pagecount(file, dest_path):
    # Download the file to get the page count
    request = drive.files().get_media(fileId=file['id'], supportsAllDrives=True)
    with io.FileIO(dest_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

# Removes duplicates by 'name'
# Keeps the first, removes those at the end
def remove_duplicate_dicts(data):
    seen = set()
    to_remove = []
    for i in range(len(data)):
        if data[i]["name"] in seen:
            to_remove.append(i)
        else:
            seen.add(data[i]["name"])
    for i in reversed(to_remove):
        del data[i]

def insert_setlist_songs_into_songlist(setlist_songs, songs):
    setlist_index = []
    for setlist_song in setlist_songs:
        found = False
        for i in range(len(songs)):
            if songs[i]['name'] == setlist_song['name']:
                songs[i] = setlist_song
                found = True
                # print("Inserting setlist index found " + i)
                setlist_index.append(i)
                break
        if not found:
            songs.append(setlist_song)
            setlist_index.append(len(songs)-1)
            # print("Inserting setlist index found " + i)
    return setlist_index

# Check to see if we have a local copy of the file, or if that file is out of date
def needs_download(local_dir, filename, gd_modified_ms):
    local_dir = pathlib.Path(local_dir)
    local_path = local_dir / f"{filename}"

    # If file does not exist, we must download it
    if not local_path.exists():
        return True

    # Convert Drive timestamp (ms) → seconds → local float timestamp
    gd_ts = gd_modified_ms / 1000.0

    # Get local modification timestamp
    local_ts = local_path.stat().st_mtime

    # If Google Drive version is newer, download
    return gd_ts > local_ts

# Create a separate .db file for each part
# We start with an empty MobileSheets database created from the app
# This schema might change with future updates to the app, so we might have to update this script.
def update_database(songs, setlists, part_folders):
    # Create database files
    used_instruments = set()
    for song in songs:
        for part in song['parts']:
            if part not in used_instruments:
                used_instruments.add(part)
    with Live(log_indent + "Creating fresh databases...", console=console, refresh_per_second=4) as live:
        clear_output_folder(live)
        for instrument in used_instruments:
            create_database(instrument, live=live)

            # Initialize setlists
            db_path = 'output/' + instrument.replace(' ','_').lower() + '.db'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()

            now_ms = int(time.time() * 1000)
            for setlist in setlists:
                if args.verbose:
                    print("Creating Setlist [cyan]" + setlist['name'] + "[/cyan] in db [cyan]" + db_path, live=live)
                cur.execute("""
                INSERT INTO Setlists (Name, LastPage, LastIndex, SortBy, Ascending, DateCreated, LastModified)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (setlist['name'], 0, 0, 0, 1, now_ms, now_ms))
            
            conn.commit()
            conn.close()
        print("Fresh databases created!", live=live)

    # Download files to count pages
    push_log_section("[cyan]Downloading songs to count pages and assembling MobileSheets database...")
    with Live(log_indent + "Downloading...", console=console, refresh_per_second=4) as live:
        for song_idx in range(len(songs)):
            song = songs[song_idx]
            os.makedirs("cache", exist_ok=True)
            os.makedirs("cache/pdf", exist_ok=True)
            
            for file in song['files']:
                file_name_sanitized = file['name'].replace(' ', '_').replace('\\', '_').replace('/','_')
                file_cache_path = "cache/pdf/" + file_name_sanitized

                if needs_download("cache/pdf", file_name_sanitized, file["modifiedTime"]):
                    print('Downloading and caching PDF to count pages for [green]' + file['name'], live=live)
                    download_pdf_for_pagecount(file, "cache/pdf/" + file_name_sanitized)
                else:
                    if args.verbose:
                        print('Using cached PDF for [green]' + file_name_sanitized, live=live)
                file['pagecount'] = get_page_count(file_cache_path)
                file['pageorder'] = '1-' + str(file['pagecount'])
    pop_log_section()

    for part in used_instruments:
        push_log_section(f"[cyan]Assembling database for [magenta]{part}")
        with Live(log_indent + "Opening database...", console=console, refresh_per_second=4) as live:
            # Open database
            db_path = 'output/' + part.replace(' ','_').lower() + '.db'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            # A dict of song_idx to a dict of filename to song ID
            #   song_idx: the index of the song in our python song list
            #   song_id: the SongId field in the MobileSheets sqlite3 database
            song_id = 0
            song_ids = {}
            for song_idx in range(len(songs)):
                song = songs[song_idx]
                if part in song['parts']:
                    song_ids[song_idx] = {}
                    for file in song['parts'][part]:
                        song_id += 1
                        song_ids[song_idx][file['name']] = song_id

                        if args.verbose:
                            print("Inserting Song [green]" + file['name'] + '[/green] into database [cyan]' + db_path, live=live)
                        
                        if 'preferred_name' not in file:
                            print("File did not have preferred name:")
                            print(file)

                        # The file names are ugly. We can change the name in the MobileSheets database without changing the file name.
                        cur.execute("""
                        INSERT INTO Songs (Title, Difficulty, LastPage, OrientationLock, Duration, Stars, VerticalZoom, Sharpen, SharpenLevel, CreationDate, LastModified, Keywords, AutoStartAudio, SongId)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (file['preferred_name'], 0, 0, 0, 0, 0, 1.0, 0, 7, file['createdTime'], file['modifiedTime'], "", 0, 0))

                        cur.execute("""
                        INSERT INTO Files (SongId, Path, PageOrder, FileSize, LastModified, Source, Type, SourceFilePageCount, FileHash, Width, Height)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (song_id, part_folders[part]['id'] + '/' + file['name'], file['pageorder'], file['size'], file['modifiedTime'], 1, 1, file['pagecount'], file['filehash'], -1, -1))

                        cur.execute("""
                        INSERT INTO AutoScroll (SongId, Behavior, PauseDuration, Speed, FixedDuration, ScrollPercent, ScrollOnLoad, TimeBeforeScroll)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (song_id, 0, 8000, 3, 1000, 20, 0, 2000))

                        for i in range(file['pagecount']):
                            cur.execute("""
                            INSERT INTO Crop (SongId, Page, Left, Top, Right, Bottom, Rotation)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            (song_id, i, 0, 0, 0, 0, 0))

                        for i in range(file['pagecount']):
                            cur.execute("""
                            INSERT INTO ZoomPerPage (SongId, Page, Zoom, PortPanX, PortPanY, LandZoom, LandPanX, LandPanY, FirstHalfY, SecondHalfY)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (song_id, i, 100.0, 0, 0, 100.0, 0, 0, 0, 0))

                        cur.execute("""
                        INSERT INTO MetronomeSettings (SongId, Sig1, Sig2, Subdivision, SoundFX, AccentFirst, AutoStart, CountIn, NumberCount, AutoTurn)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (song_id, 2, 0, 0, 0, 0, 0, 0, 1, 0))

                        for i in range(file['pagecount']):
                            cur.execute("""
                            INSERT INTO MetronomeBeatsPerPage (SongId, Page, BeatsPerPage)
                            VALUES (?, ?, ?)""",
                            (song_id, i, 0))

                        # cur.execute("""
                        # INSERT INTO ZoomPerPage ()
                        # VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        # (1, 0, 8000, 3, 1000, 20, 0, 2000))

                        # Add line to hashcodes
                        with open('output/'+part.replace(' ','_').lower() + '_hashcodes.txt', "a", encoding="utf-8") as f_out:
                            f_out.write(f"{part_folders[part]['id']}/{file['name']}\n")
                            f_out.write(f"{file['filehash']}\n")
                            f_out.write(f"{file['modifiedTime']}\n")
                            f_out.write(f"{file['size']}\n")

            for i in range(len(setlists)):
                setlist = setlists[i]
                setlist_id = i+1 # 1-indexed
                found = False
                for setlist_song_idx in setlist['song_index']:
                    setlist_song = songs[setlist_song_idx]
                    if part in setlist_song['parts']:
                        for setlist_file in setlist_song['parts'][part]:
                            song_id = song_ids[setlist_song_idx][setlist_file['name']]
                            cur.execute("""
                            INSERT INTO SetlistSong (SetlistId, SongId)
                            VALUES (?, ?)""",
                            (setlist_id, song_id))
                            found = True
                            if args.verbose:
                                print("Inserting Setlist Song [green]" + setlist_file['name'] + "[/green] into setlist [cyan]" + setlist['name'], live=live)
            
            conn.commit()
            conn.close()
            print(f"Finished assembling database. Added [cyan]{song_id}[/cyan] songs", live=live)
            pop_log_section()

    pop_log_section()
    for instrument in used_instruments:
        db_name = instrument.replace(' ','_').lower() + '.db'
        hashcodes_name = instrument.replace(' ','_').lower() + '_hashcodes.txt'
        push_log_section('Uploading [cyan]output/' + db_name + '[/cyan] and [cyan]' + hashcodes_name + '[/cyan] to [green]' + instrument)
        part_folder_id = part_folders[instrument]['id']
        with Live(log_indent + "Uploading...", console=console, refresh_per_second=4) as live:
            upload_to_drive(local_path='output/'+db_name, dest_name='mobilesheets.db', parent_folder_id = part_folder_id, live=live)
            upload_to_drive(local_path='output/'+hashcodes_name, dest_name='mobilesheets_hashcodes.txt', parent_folder_id = part_folder_id, live=live)
            print("Uploaded!", live=live)
        pop_log_section()

try:
    main()
finally:
    # Save log
    log_indent = ''
    print("Output saved to log.html and log.txt")
    file_console.save_html("log.html")
    file_console.save_text("log.txt")
    log_path = os.path.abspath("log.html")
    webbrowser.open(f"file://{log_path}")
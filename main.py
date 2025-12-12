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
from dateutil import parser
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Relevant Google Drive folders
WEEKLY_AGENDA_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"
MEMORIZATION_LIST_ID = "1lYz54_jarIxfqZu0vVebfcRIBykGsikdSs3QNhlecU0" # TODO - this just links to mp3s, not song folders, maybe remove
DEST_MUSIC_FOLDER = "1h-T2mnFrr0VpafBDJ3nv3vvO_xLGir9t" # The folder where the MobileSheets database and PDFs will end up
SRC_MUSIC_FOLDER = "12y2cjGE7GE3MTJ8QtNs3_Z5L30o5Ql6D" # The LTBB folder containing all the sheet music. Currently organized in folders like 'A-C', 'D-F', etc
SEASONAL_SONGS = "1M7sLr9wwvHJIfKGijRTSjC5ae1CODzbY" # Some subfolders that contain additional songs not in the alphabetic folders
DRIVE_ID = "0AIscw8ywGnshUk9PVA" # Quirk of using a Shared Drive, we sometimes need this

# Warning - if the 5. Resources folder name ever changes, we are at risk of some terrible infinite recursion because this script uploads files there
IGNORE_FOLDERS = ['1. Member Drafts', '2. Seasonal Songs', '3. Warm-ups', '4. 3rd Rail Drumline', '5. Resources', '6. Recordings']
MAX_SONGS = 99999

# Set up Google Drive 
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/documents.readonly"]

console = Console()
builtins.print = lambda *args, **kwargs: console.print(*args, highlight=False, **kwargs)
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--clean', action="store_true", help="Force a clean redownload of songs, clear local cache. This does NOT clean the Google Drive folder, do that manually if needed (it shouldn't be needed).") 
arg_parser.add_argument('--skipquery', action="store_true", help="Used for inner dev loop. Stores the file metadata of the drive so we don't have to requery each song. But usually you want to requery.")
arg_parser.add_argument('--verbose', action="store_true", help="Spit out extra info") 
arg_parser.add_argument('--dedupe', action="store_true", help="For use when a part folder accidentally ends up with multiple copies of the same file. Shouldn't happen.") 
args = arg_parser.parse_args()

drive_lock = Lock()

def get_creds():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

# Build Drive
creds = get_creds()
docs = build("docs", "v1", credentials=creds)
drive = build("drive", "v3", credentials=creds)

# TODO
# - if no instruments:
#   - Horn means F Horn
#   - take any Bb/Eb/F/etc when no instruments available

# Instrument list
instruments = {
    'Score': ['Score'],
    'Tuba': ['Tuba', 'Sousaphone', 'Sousa', 'Euphonium', 'Euph', 'Low Brass', 'Basses', 'Bass (Trebel Clef)', 'Bass_Line'],
    'Horn': ['Horn in F', 'F Horn', 'Mellophone', 'Horns F'],
    'Percussion': ['Percussion', 'Drum', 'Snareline', 'Perc', 'BassDr', 'Snare', 'Congo', 'Toms', 'Quads', 'Cymbal', 'Glockenspiel'],
    'Clarinet': ['Clarinet'],
    'Soprano Sax': ['Soprano'],
    'Tenor Sax': ['Tenor'],
    'Alto Sax': ['Alto'],
    'Bass Sax': ['Bass Sax', 'Bass Saxophone'],
    'Bari Sax': ['Bari'],
    'Trumpet': ['Trumpet', 'Flugelhorn', 'Trmp', 'Trumplet'],
    'Trombone': ['Trombone', 'Tbn', 'Trmb', 'Bone'],
    'Eb Horn' : ['Eb Horn', 'Horn in Eb'],
    'Flute' : ['Flute', 'C Woodwind']
}
part_folders = {}
flat_instrument_list = [sub for main in instruments for sub in instruments[main]]
instrument_lookup = {}
for main in instruments:
    for sub in instruments[main]:
        instrument_lookup[sub.lower()] = main

def save_dict(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_dict(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def query_drive_files(query, fields):
    page_token = None
    files = []

    # Need to include nextPageToken because sometimes there are more than 100 files.
    fields = f"nextPageToken, {fields}"

    while True:
        response = drive.files().list(
            q=query,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
            pageToken=page_token,
            fields=fields
        ).execute()

        # Get the next page of files
        files.extend(response['files'])
        page_token = response.get('nextPageToken')

        if not page_token:
            break
    return files

def list_folders_in_folder(folder_id):
    return query_drive_files(
        query=f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name)",
    )

def list_pdfs_in_folder(folder_id):
    files = query_drive_files(
        query=f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false",
        fields="files(id, name, size, createdTime, modifiedTime, parents)"
    )

    # Populate extra metadata we will need
    for file in files:
        file['filehash'] = java_string_hashcode(file['name'])
        dt = parser.isoparse(file['modifiedTime'])
        file['modifiedTime'] = int(dt.timestamp() * 1000)
        dt = parser.isoparse(file['createdTime'])
        file['createdTime'] = int(dt.timestamp() * 1000)
    return files

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
    for folder in top_level_folders:
        print("    Querying folder [green]" + folder['name'])
        folders += list_folders_in_folder(folder['id'])
        i+=1
        if i >= MAX_SONGS:
            break

    # Get PDFs
    folders.sort(key=lambda folder: folder['name'])
    i = 0
    for folder in folders:
        print("    Assembling song [green]" + folder['name'])
        folder['files'] = list_pdfs_in_folder(folder['id'])
        i += 1
        if i >= MAX_SONGS:
            break
    
    folders = [folder for folder in folders if 'files' in folder and len(folder['files']) > 0]

    return folders

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

# Figure out instrumentation from song titles and which files belong to which instrument
def assemble_song_parts(song):
    files = song['files']
    song['parts'] = {}
    for file in files:
        file_name = file['name']
        instruments = extract_instruments(file_name)
        for instrument in instruments:
            if instrument not in song['parts']:
                song['parts'][instrument] = []
            song['parts'][instrument].append(file)
            print("        [magenta]" + instrument + "[/magenta]: [green]" + file['name'])

no_instrument_files = []

# Function for getting a sanitized instrument name
def extract_instruments(file_name):
    if not file_name.endswith('.pdf'):
        return []
    instruments = []
    file_name_sanitized = file_name.lower().replace(' ', '_').replace('.','')
    for possible_instrument in instrument_lookup:
        if possible_instrument.replace(' ', '_') in file_name_sanitized and instrument_lookup[possible_instrument] not in instruments:
            instruments.append(instrument_lookup[possible_instrument])
    if not instruments:
        if 'horn' in file_name_sanitized:
            instruments.append(instrument_lookup['horn in f'])
    if not instruments:
        print("        [yellow]INSTRUMENT NOT FOUND: " + file_name)
        no_instrument_files.append(file_name)
    return instruments

def get_folder_name(folder_id):
    return drive.files().get(
        fileId=folder_id,
        fields="id, name",
        supportsAllDrives=True
    ).execute()['name']

def extract_folder_id(url):
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None

# Scrapes a doc and extracts all songs linked
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
        if not folder_contains_pdfs(folder_id):
            continue
        folder_name = get_folder_name(folder_id)
        print('    Found folder in doc: [green]' + folder_name)
        folder = {'id': folder_id, 'name': folder_name, 'files': list_pdfs_in_folder(folder_id)}
        
        if 'files' in folder and len(folder['files']) > 0:
            songs.append(folder)
    
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
    print("Created folder ", name, ":", folder['id'])

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
        part_folders[name] = files[0]
        return files[0]   # already exists

    # otherwise create it
    file = create_folder(name=name, parent_id=parent_id)
    part_folders[name] = file
    return file


def escape_drive_query(name):
    # escape single quotes by doubling them
    return name.replace("'", "\\'")

# Drive copy file if newer
def sync_file(source_file, dest_folder, existing_file=None, new_name=None):
    # Get source file metadata
    source_name = new_name or source_file["name"]
    source_modified = source_file["modifiedTime"]

    if existing_file:
        existing_modified = existing_file["modifiedTime"]

        # Compare modified timestamps
        if source_modified > existing_modified:
            print(f"    Source file is newer. Replacing '{source_name}'")
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
                print(f"    Existing file '{source_name}' is up-to-date. Skipping copy.")
            return existing_file["id"]  # nothing to do

    # Copy the source file into the folder
    new_file_metadata = {"parents": [dest_folder['id']], "name": source_name}
    copied_file = drive.files().copy(fileId=source_file['id'], body=new_file_metadata, fields="id, name", supportsAllDrives=True).execute()
    print(f"    Copied '[green]{source_name}[/green]' to folder '[magenta]{dest_folder['name']}[/magenta]'")
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
def copy_songlist_into_drive(songs):
    for song in songs:
        if song is None:
            print('[red]ERROR - no song!')

        if args.verbose:
            print("Copying files for [green]" + song['name'])
        for part_key in song['parts']:
            files = song['parts'][part_key]
            # Some parts have more than one chart (trumpet 1/2), so copy all files
            for file in files:
                needs_copy = True
                existing_dest_file = None
                for dest_file in part_folders[part_key]['files']:
                    if dest_file['name'] == file['name']:
                        if args.verbose:
                            print("    Found an existing file for [green]" + file['name'])
                        existing_dest_file = dest_file
                if not existing_dest_file:
                    if args.verbose:
                        print("     No existing Drive file found in destination folder for [green]" + file['name'])    

                copied = sync_file(
                    source_file=file,
                    dest_folder=part_folders[part_key],
                    existing_file=existing_dest_file,
                    new_name=file['name']
                )

def upload_to_drive(local_path, dest_name, parent_folder_id):
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
        if args.verbose:
            print(f"    Deleting old {f['name']} ({f['id']})")
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

    if args.verbose:
        print(f"    Uploaded {uploaded['name']} ({uploaded['id']})")
    return uploaded["id"]

def clear_output_folder():
    folder = 'output'
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)
            if args.verbose:
                print(f"    Deleted {path}")

def create_database(db_name):
    db_path = 'output/' + db_name.replace(' ','_').lower() + '.db'
    if os.path.exists(db_path):
        print('    Removing old ' + db_path + ' and replacing with a blank fresh library db')
        os.remove(db_path)
    if args.verbose:
        print('    Created ' + db_path)
    shutil.copy("ltbb_blank.db", db_path)

def java_string_hashcode(s: str) -> int:
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h & 0x80000000:
        h = -((~h + 1) & 0xFFFFFFFF)
    return h

def get_page_count(path):
    reader = PdfReader(path)
    return len(reader.pages)

def download_pdf_for_pagecount(file, dest_path):
    # Download the file to get the page count
    request = None
    with drive_lock:
        request = drive.files().get_media(fileId=file['id'], supportsAllDrives=True)
    with io.FileIO(dest_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

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

def update_database(songs, setlists):
    # Create database files
    clear_output_folder()
    used_instruments = set()
    for song in songs:
        for part in song['parts']:
            if part not in used_instruments:
                used_instruments.add(part)
    for instrument in used_instruments:
        create_database(instrument)

        # Initialize setlists
        db_path = 'output/' + instrument.replace(' ','_').lower() + '.db'
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        now_ms = int(time.time() * 1000)
        for setlist in setlists:
            cur.execute("""
            INSERT INTO Setlists (Name, LastPage, LastIndex, SortBy, Ascending, DateCreated, LastModified)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (setlist['name'], 0, 0, 0, 1, now_ms, now_ms))
        
        conn.commit()
        conn.close()

    part_song_ids = {}
    for part in used_instruments:
        part_song_ids[part] = 0

    # TODO - get songs that belong in setlists and insert them into the Setlistsong table
    # Also insert entries into setlists table per setlist
    print("[cyan]Downloading songs to count pages...")
    for song in songs:
        # print("Downloading files (to count pages) for [green]" + song['name'])
        os.makedirs("cache", exist_ok=True)
        os.makedirs("cache/pdf", exist_ok=True)
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = []
            for file in song['files']:
                file_name_sanitized = file['name'].replace(' ', '_').replace('\\', '_').replace('/','_')
                file_cache_path = "cache/pdf/" + file_name_sanitized

                if needs_download("cache/pdf", file_name_sanitized, file["modifiedTime"]):
                    print('    Downloading and caching PDF for [green]' + file['name'])
                    download_pdf_for_pagecount(file, "cache/pdf/" + file_name_sanitized) # TODO - don't download if we have a copy
                else:
                    if args.verbose:
                        print('    Using cached PDF for [green]' + file_name_sanitized)                
                file['pagecount'] = get_page_count(file_cache_path)
                file['pageorder'] = '1-' + str(file['pagecount'])

        for part in song['parts']:
            db_path = 'output/' + part.replace(' ','_').lower() + '.db'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()

            for file in song['parts'][part]:
                part_song_ids[part] += 1
                song_id = part_song_ids[part]
                # print("    Inserting Song [green]" + file['name'])
                
                cur.execute("""
                INSERT INTO Songs (Title, Difficulty, LastPage, OrientationLock, Duration, Stars, VerticalZoom, Sharpen, SharpenLevel, CreationDate, LastModified, Keywords, AutoStartAudio, SongId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (file['name'][:-4], 0, 0, 0, 0, 0, 1.0, 0, 7, file['createdTime'], file['modifiedTime'], "", 0, 0))

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

                # Assume ID is correct here
                for i in range(len(setlists)):
                    setlist = setlists[i]
                    setlist_id = i+1 # 1-indexed
                    found = False
                    for setlist_song in setlist['songs']:
                        if found:
                            break
                        for setlist_file in setlist_song['files']:
                            if setlist_file['name'] == file['name']:
                                cur.execute("""
                                INSERT INTO SetlistSong (SetlistId, SongId)
                                VALUES (?, ?)""",
                                (setlist_id, song_id))
                                found = True
                                break

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

            conn.commit()
            conn.close()

    print("[cyan]Done downloading!")
    for instrument in used_instruments:
        db_name = instrument.replace(' ','_').lower() + '.db'
        hashcodes_name = instrument.replace(' ','_').lower() + '_hashcodes.txt'
        print('Uploading [cyan]output/' + db_name + '[/cyan] and [cyan]' + hashcodes_name + '[/cyan] to [green]' + instrument)
        part_folder_id = part_folders[instrument]['id']
        upload_to_drive(local_path='output/'+db_name, dest_name='mobilesheets.db', parent_folder_id = part_folder_id)
        upload_to_drive(local_path='output/'+hashcodes_name, dest_name='mobilesheets_hashcodes.txt', parent_folder_id = part_folder_id)


### Main script execution starts here ###

# Clean cache
if args.clean:
    if os.path.exists('cache'):
        shutil.rmtree('cache')

# Assemble song list
songs = []
cache = load_dict('cache/cache.json')
if args.skipquery and cache and 'songs' in cache and 'setlists' in cache:
    # For inner dev loop, we can skip the query of the google drive folders and docs
    print('[cyan]Loading songs from cached file!')
    songs = cache['songs']
    setlists = cache['setlists']
else:
    print("[cyan]Querying LTBB Drive...")
    songs = query_tree([SRC_MUSIC_FOLDER, SEASONAL_SONGS])
    print("[cyan]Done querying!")
    time.sleep(1)

    # Read the rehearsal schedule
    # TODO - memorization list actually does not link to any sheet music
    setlists = []
    # setlist_docs = {"Memorization List": MEMORIZATION_LIST_ID, "Rehearsal": WEEKLY_AGENDA_ID}
    setlist_docs = {"Rehearsal": WEEKLY_AGENDA_ID}

    for setlist_name in setlist_docs:
        print()
        print('[cyan]Assembling song metadata from Doc ' + setlist_name)
        setlist_doc_id = setlist_docs[setlist_name]
        setlist_songs = scrape_song_list(setlist_doc_id)
        remove_duplicate_dicts(setlist_songs)
        # Update songs. If there are conflicts, use the one in the doc
        songs = setlist_songs + songs
        seen = set()
        unique_songs = []
        for song in songs:
            if song["name"] not in seen:
                unique_songs.append(song)
                seen.add(song["name"])
        songs = unique_songs

        # Assemble setlist by name
        setlist = {}
        setlist['name'] = setlist_name
        setlist['songs'] = setlist_songs
        setlists.append(setlist)
    remove_duplicate_dicts(songs)

time.sleep(1)

# Save before adding parts, we'll let that happen every time in case we want to change the schema
os.makedirs('cache', exist_ok=True)
os.makedirs('output', exist_ok=True)
save_dict('cache/cache.json', {'songs':songs, 'setlists':setlists})

print()
print("[cyan]Assembling part information...")
for song in songs:
    print("    Assembling part information for [green]" + song['name'])
    # Get part information:
    assemble_song_parts(song)
print("[cyan]Part information assembled!")

if len(no_instrument_files) > 0:
    print('[yellow]Some files had no instruments:')
    for file in no_instrument_files:
        print(    '[yellow]    ' + file)

print("[cyan]See part information at [green]cache/songs_with_parts.json")
save_dict('cache/songs_with_parts.json', songs)
time.sleep(1)

# Populate folder IDs
print()
print("[cyan]Getting destination part folder IDs...")
for part in instruments:
    print("    Finding part folder [magenta]" + part)
    # Populates part_folders
    folder = get_or_create_folder(part, DEST_MUSIC_FOLDER)
    folder['files'] = list_pdfs_in_folder(folder['id'])
    print("        Found " + str(len(folder['files'])) + " existing PDFs")

if args.dedupe:
    print("[cyan]Deduping files, because somehow Geoffrey ended up getting multiple copies of the same PDF into a part folder.")
    for part in part_folders:
        dedupe_files(part_folders[part])

# Copy files from Src drive folder to Destination drive folder 
# Skips if the Src song is not newer than the Dest song
print()
print('[cyan]Copying songs into Google Drive folders...')
copy_songlist_into_drive(songs)
print('[cyan]Songs copied into Drive!')
time.sleep(1)
    

print()
print("[cyan]Updating databases...")
update_database(songs, setlists)
print("[cyan]Database updated!")
time.sleep(1)


# drive = build("drive", "v3", credentials=creds)

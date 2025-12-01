from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re
from pprint import pprint
import os
import shutil
import sqlite3
import io
from pathlib import Path
import requests
from rich.console import Console
import builtins

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/documents.readonly"]

console = Console()
builtins.print = lambda *args, **kwargs: console.print(*args, highlight=False, **kwargs)

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

def extract_folder_id(url):
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None

# Build Drive
creds = get_creds()
docs = build("docs", "v1", credentials=creds)
drive = build("drive", "v3", credentials=creds)

# Weekly agenda
WEEKLY_AGENDA_ID = "1jcazpKFV5wNjzDY-3oC9BKei_3iVQhrQ6xy3P76G-5w"
DEST_MUSIC_FOLDER = "1rGkyWusZDKKIk9gQAOMNpind1Oh95Zjb"
SRC_MUSIC_FOLDER = "12y2cjGE7GE3MTJ8QtNs3_Z5L30o5Ql6D"

# Instrument list
instruments = {
    'Score': ['Score'],
    'Tuba': ['Tuba', 'Sousaphone', 'Euphonium', 'Euph T.C.', 'Euph (T.C.)', 'Euph TC'],
    'Horn': ['Horn in F', 'F Horn', 'Mellophone'],
    'Percussion': ['Percussion', 'Drum', 'Snareline', 'Perc 1', 'Perc 2'],
    'Clarinet': ['Clarinet'],
    'Tenor Sax': ['Tenor'],
    'Alto Sax': ['Alto'],
    'Bass Sax': ['Bass Sax', 'Bass Saxophone'],
    'Bari Sax': ['Bari'],
    'Trumpet': ['Trumpet', 'Flugelhorn'],
    'Trombone': ['Trombone']
}
# instruments = {main.lower() : [sub.lower() for sub in instruments[main]] for main in instruments}
flat_instrument_list = [sub for main in instruments for sub in instruments[main]]
instrument_lookup = {}
for main in instruments:
    for sub in instruments[main]:
        instrument_lookup[sub.lower()] = main

def list_files(folder_id):
    query = f"'{folder_id}' in parents"
    result = drive.files().list(q=query, fields="files(id, name)").execute()
    return result.get("files", [])

def folder_contains_pdfs(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/pdf' and trashed = false"
    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1  # we only need to know if at least one exists
    ).execute()
    
    return len(results.get("files", [])) > 0

# Figure out instrumentation from song titles and which files belong to which instrument
def assemble_song_parts(song):
    files = song['files']
    song['parts'] = {}
    for file in files:
        file_name = file['name']
        instrument = extract_instrument(file_name)
        if instrument is None:
            continue
        if instrument not in song['parts']:
            song['parts'][instrument] = []
        song['parts'][instrument].append(file) 

# Function for getting a sanitized instrument name
def extract_instrument(file_name):
    if not file_name.endswith('.pdf'):
        return None
    file_name = file_name[:-4]
    if "-" not in file_name:
        print("    [yellow]NO DASH - skipping " + file_name)
        return None
    file_split = file_name.split('-')
    if len(file_split) != 2:
        print("    [yellow]Split on dash didn't work, skipping ", file_name)
        return None
    instrument = None
    unsanitized_instruments = []
    unsanitized_instruments.append(file_split[0].lower().strip())
    unsanitized_instruments.append(file_split[1].lower().strip())
    for unsanitized_instrument in unsanitized_instruments:
        for possible_instrument in instrument_lookup:
            possible_instrument = possible_instrument.lower()
            if unsanitized_instrument in possible_instrument or possible_instrument in unsanitized_instrument:
                instrument = instrument_lookup[possible_instrument]
                break
        if instrument != None:
            break
    if instrument == None:
        print("    [yellow]INSTRUMENT NOT FOUND: " + file_name)
        return None
    return instrument

def get_folder_name(folder_id):
    return drive.files().get(
        fileId=folder_id,
        fields="id, name"
    ).execute()['name']

def assemble_song_from_folder(folder_id):
    if not folder_contains_pdfs(folder_id):
        return None
    song = {}
    song['title'] = get_folder_name(folder_id)
    song['files'] = list_files(folder_id)

    assemble_song_parts(song)
    return song

# Scrapes a doc and extracts all songs linked
def scrape_song_list(doc_id):
    print()
    print('[cyan]Assembling song metadata from Doc')
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
    links = links[:2]

    songs = []
    # Get files at Drive links
    for link in links:
        folder_id = extract_folder_id(song["link"])
        song = assemble_song_from_folder(folder_id)
    return songs

def list_subfolders(parent_folder_id):
    query = (
        f"'{parent_folder_id}' in parents "
        "and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        pageSize=1000  # adjust if needed
    ).execute()
    
    return results.get("files", [])

IGNORE_FOLDERS = ['1. Member Drafts', '3. Warm-ups', '4. 3rd Rail Drumline', '5. Resources', '6. Recordings']

def scrape_folder_of_songs(folder_id):
    print()
    print('[cyan]Assembling song metadata from folders')
    songs = []
    i = 0
    for cat_folder in list_subfolders(folder_id):
        if cat_folder['name'] in IGNORE_FOLDERS:
            continue
        if i > 3:
            break
        for song_folder in list_subfolders(cat_folder['id']):
            if i > 3:
                break
            print("Assembling song metadata [bold green]'" + song_folder['name'] + "'[/bold green] with ID '" + song_folder['id'] + "'")
            song = assemble_song_from_folder(song_folder['id'])
            songs.append(song)
            i += 1
    return songs

# Drive Create folder
def create_folder(name, parent_id=None):
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder"
    }
    if parent_id:
        file_metadata["parents"] = [parent_id]

    folder = drive.files().create(
        body=file_metadata,
        fields="id, name, parents"
    ).execute()
    print("Created folder ", name, ":", folder['id'])

    return folder

# Drive Create folder if does not exist
def get_or_create_folder(name, parent_id=None):
    query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = drive.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive"
    ).execute()

    files = results.get("files", [])
    if files:
        return files[0]   # already exists

    # otherwise create it
    return create_folder(name=name, parent_id=parent_id)


def escape_drive_query(name):
    # escape single quotes by doubling them
    return name.replace("'", "\\'")

# Drive copy file if newer
def sync_file(source_file_id, dest_folder_id, new_name=None):
    # Get source file metadata
    source = drive.files().get(
        fileId=source_file_id,
        fields="id, name, modifiedTime"
    ).execute()
    source_name = new_name or source["name"]
    source_modified = source["modifiedTime"]

    # Check if a file with the same name exists in destination
    query = f"name contains '{escape_drive_query(source_name)}' and '{dest_folder_id}' in parents and trashed = false"
    results = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    existing_files = results.get("files", [])

    if existing_files:
        existing = existing_files[0]
        existing_modified = existing["modifiedTime"]

        # Compare modified timestamps
        if source_modified > existing_modified:
            print(f"        Source file is newer. Replacing '{source_name}'")
            # Delete the old copy
            drive.files().delete(fileId=existing["id"]).execute()
        else:
            print(f"        Existing file '{source_name}' is up-to-date. Skipping copy.")
            return existing["id"]  # nothing to do

    # Copy the source file into the folder
    new_file_metadata = {"parents": [dest_folder_id], "name": source_name}
    copied_file = drive.files().copy(fileId=source_file_id, body=new_file_metadata, fields="id, name").execute()
    print(f"Copied '{source_name}' to folder")
    return copied_file["id"]

def get_file_metadata(file_id):
    return drive.files().get(
        fileId=file_id,
        fields="id, name, mimeType, size, createdTime, modifiedTime, md5Checksum, parents"
    ).execute()

def copy_songlist_into_drive(songs):
    print()
    print('[cyan]Copying songs into Google Drive folders')
    # Make copies of files to my Drive
    for song in songs:
        print("Copying files for [green]" + song['title'])
        for part_key in song['parts']:
            part_charts = song['parts'][part_key]
            print("    Instrument [magenta]" + part_key)
            # Some parts have more than one chart (trumpet 1/2)
            for part_chart in part_charts:
                part_folder = get_or_create_folder(part_key, DEST_MUSIC_FOLDER)
                copied = sync_file(
                    source_file_id=part_chart['id'],
                    dest_folder_id=part_folder['id'],
                    new_name=part_chart['name']
                )

            # print("New file ID:", copied)

def clear_ouptput_folder():
    folder = 'output'
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            os.remove(path)
            print(f"[cyan]Deleted {path}")

def create_database(db_name):
    db_path = 'output/' + db_name + '.db'
    if os.path.exists(db_path):
        print('    [cyan]Removing old ' + db_path + ' and replacing with a blank fresh library db')
        os.remove(db_path)
        shutil.copy("ltbb_blank.db", db_path)

def update_database(songs):
    print()
    print("[cyan]Updating database")
    # Database time
    db_path = "ltbb.db"

    # cur.execute("CREATE TABLE Songs(Id INTEGER PRIMARY KEY,Title VARCHAR(255),Difficulty INTEGER,Custom VARCHAR(255) DEFAULT '',Custom2 VARCHAR(255) DEFAULT '',LastPage INTEGER,OrientationLock INTEGER,Duration INTEGER,Stars INTEGER DEFAULT 0,VerticalZoom FLOAT DEFAULT 1,SortTitle VARCHAR(255) DEFAULT '',Sharpen INTEGER DEFAULT 0,SharpenLevel INTEGER DEFAULT 4,CreationDate INTEGER DEFAULT 0,LastModified INTEGER DEFAULT 0,Keywords VARCHAR(255) DEFAULT '',AutoStartAudio INTEGER,SongId INTEGER)")
    # cur.execute("CREATE TABLE Files(Id INTEGER PRIMARY KEY,SongId INTEGER,Path VARCHAR(255),PageOrder VARCHAR(255),FileSize INTEGER,LastModified INTEGER,Source INTEGER,Type INTEGER,Password VARCHAR(255) DEFAULT '',SourceFilePageCount INTEGER,FileHash INTEGER,Width INTEGER,Height INTEGER)")
    # cur.execute("CREATE INDEX files_song_id_idx ON Files(SongId)")

    # TODO create DBs for each instrument
    pprint(songs)
    used_instruments = {}
    for song in songs:
        for part in parts:
            if part not in used_instruments:
                used_instruments.add(part)

    for instrument in used_instruments:
        create_database(instrument)

    song_id = 0
    for song in songs:
        for file in song['files']:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()

            metadata = get_file_metadata(file['id'])
            if metadata['mimeType'] != 'application/pdf':
                continue
            file['lastmodified'] = metadata['modifiedTime']
            file['creationdate'] = metadata['createdTime']
            file['filesize'] = int(metadata['size'])
            file['pagecount'] = 1 # TODO
            file['pageorder'] = '1-1' # TODO
            file['filehash'] = metadata['md5Checksum']

            song_id += 1
            print("Inserting Song [green]" + file['name'])
            
            # cur.execute("""
            # INSERT INTO Songs (Title, CreationDate, LastModified)
            # VALUES (?, ?, ?)""",
            # (song['title'], 1234567, 1234567))
            cur.execute("""
            INSERT INTO Songs (Title, Difficulty, LastPage, OrientationLock, Duration, Stars, VerticalZoom, Sharpen, SharpenLevel, CreationDate, LastModified, Keywords, AutoStartAudio, SongId)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (file['name'][:-4], 0, 0, 0, 0, 0, 1.0, 0, 7, file['creationdate'], file['lastmodified'], "", 0, 0))

            # TODO - none of these properties exist yet
            cur.execute("""
            INSERT INTO Files (Path, PageOrder, FileSize, LastModified, Source, Type, SourceFilePageCount, FileHash, Width, Height)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (file['name'], file['pageorder'], file['filesize'], file['lastmodified'], 1, 1, file['pagecount'], file['filehash'], -1, -1))

            conn.commit()
            conn.close()

# Assemble song list
songs = scrape_folder_of_songs(SRC_MUSIC_FOLDER)
# copy_songlist_into_drive(songs)
update_database(songs)


# drive = build("drive", "v3", credentials=creds)

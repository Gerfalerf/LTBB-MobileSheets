# LTBB MobileSheets

This is a project for preparing a MobileSheets database for LTBB.
1. Assembles the PDFs into a folder structure that MobileSheets understands
2. Scrapes the Weekly Rehearsal Agenda to put together a setlist in a form that MobileSheets understands
3. Uploads those databases and PDFs into a separate Google Drive folder for each instrument.

## Installation
- Requires Python 3
- Install packages in requirements.txt with pip
- Follow the [Google Workspace API](https://developers.google.com/workspace/drive/api/quickstart/python) quickstart instructions up to the step of downloading a credentials.json file into your working folder

## Running the Script
_The first time you run the script, it will take longer. This is because we have to download the PDFs in order to count the pages, which the MobileSheets database requires. Subsequent runs will be faster, as it will use this cache to skip both downloads and some Google Drive query operations for files that have not since changed in the Drive!_

1. Make sure [Geoffrey's LTBB MobileSheets](https://drive.google.com/drive/u/0/folders/1rGkyWusZDKKIk9gQAOMNpind1Oh95Zjb) folder is added to your Google Drive. (Right now you'll need edit access from Geoffrey, but we should give LTBB owner/edit access so it can dole out the permissions instead of me)
2. `python main.py`
    1. The first time you run the script, it will prompt you for permission and generate a token.json.
    2. If you haven't run the script in a while, you may need to delete token.json and regenerate it.

## Syncing with MobileSheets
1. Make sure the [LTBB MobileSheets](https://drive.google.com/drive/u/0/folders/1rGkyWusZDKKIk9gQAOMNpind1Oh95Zjb) folder is added to your Google Drive
2. Use a separate Library! MobileSheets lets you create multiple libraries under Menu -> Switch Libraries. This will keep any other music you have in MobileSheets from being clobbered by this tool.
3. In MobileSheets, go to Menu -> Sync Library and select the folder containing your instrument. The first time you sync it will take a long time, but subsequent syncs will be fast!
4. Sync once a week to get the new Rehearsal Setlist and any updated songs!

## TODO
- Transfer ownership of my 
- Commit a blank mobilesheets.db, since we don't build one from scratch.
- Some instruments should receive other instruments' parts if they don't have one. (e.g. Bass Sax should get the Tuba part). Need to speak to humans to figure this out.
  - Also, e.g. Saints has instrumentation like "Bb instruments"/"Eb instruments" which the script currently doesn't handle, but could be made to.
- Change song names automatically, so that songs of the form "Tenor Sax - Blah" are renamed to "Blah - Tenor Sax" so they are alphabetical by title.
- On auth failure, delete token.json and re-run
- Since we are doing all this anyways, it would not be hard to just add all the rehearsal PDFs to a Google Drive subfolder, since not everyone uses MobileSheets 
# Spotify Charts Data
In this repo, I collect and explore data related to tracks featured in the [Spotify Charts](https://charts.spotify.com/charts). I build on the work I did in [this repo](https://github.com/Sejmou/spotify-charts-viz.git) where I already did quite a lot of stuff. Unfortunately, the data processing in this project was really messy, so I decided to start from scratch to create a cleaner dataset.

## Data collection scripts
I have come up with CLI scripts for fetching data related to the Spotify Charts. They can be found in the `cli_scripts` subfolder of this project. As the folder name suggests, the scripts can all be invoked directly from the command line. You can call each script with the `-h` option to get information about the accepted arguments (example: `python cli_scripts/get_all.py -h`). 

### Chart Data Scraping
First, I came up with scripts for assembling data for tracks of the Spotify Daily Top 200. Unfortunately, this data is not available via the API. Furthermore, one has to download chart CSV files for each region and track separately (by navigating to the Spotify Charts page and clicking the download button) which is very inconvenient. 

However, I worked around this by creating two scripts (see the `spotify_charts` subfolder):
 - `download.py`: automates the process of downloading charts CSV files for several regions (either all or a subset specified via arguments) and a given date range (start + end date) using `selenium` (requires Spotify account/credentials!)
 - `combine_charts.py`: combines downloaded Spotify chart CSV files located in the specified directory into a single `.parquet` file

### Metadata from Spotify API
A lot of interesting information and metadata about music on Spotify can also be retrieved from Spotify's [official API](https://developer.spotify.com/documentation/web-api). All scripts using the Spotify API (via the [`spotipy`](https://github.com/spotipy-dev/spotipy) Python API wrapper) can be found in `spotify_api`:

- `get_track_metadata.py`: fetches track metadata from the `/tracks` API endpoint for unique track IDs mentioned in a provided `.parquet` file. Outputs a folder of several metadata `.parquet` files
- `get_album_metadata.py`: does the same thing as above, only for albums instead of tracks (using the `/albums` API endpoint)
- `get_artist_metadata.py`: fetches artist metadata for all unique artist IDs among several input files (each having an `artists_id` column), also storing metadata in a folder like the other scripts above
- `get_all.py`: combines all scripts, getting track metadata first, then album metadata for all albums associated with tracks and finally artist metadata for all track and album artists.

### Metadata from inofficial Spotify APIs
Unfortunately, the information for track credits (specifically, songwriters and producers) is also [not available via the public Spotify API](https://community.spotify.com/t5/Spotify-for-Developers/Getting-credits-on-a-track/td-p/4950934). However, I came up with a way to work around that. One can extract the request headers that are used for specific requests made by the Spotify Web App, e.g. when opening the `Show Credits` popup on a track page and reuse them to make other requests to the same (inofficial/internal) API endpoint.

This approach can be used to 

### Example: Create dataset around Spotify Daily Top 200 Charts for 2022

#### Pt. 1: Download Charts
```bash
# download CSVs; might take a loooong time, can be interrupted and restarted/resumed later
python cli_scripts/spotify_charts/download.py -s 2022-01-01 -e 2022-12-31 -o data/scraper_downloads

# combine downloaded CSVs into single parquet file
python cli_scripts/spotify_charts/combine.py -o data/top200_2022
```

#### Pt. 2: Metadata from the API
```bash
python cli_scripts/spotify_api/get_all.py -i data/top200_2022/charts.parquet
```

#### Pt. 3: Additional (internal) API data
TODO: add proper command once scripts are 'finished' (good enough)
```bash

```

### Addtional setup instructions
To make everything work, you can follow these instructions (assuming you have a recent version of Python installed)

If you want, you can create a new environment, e.g. with `conda`:
```
conda env create --name=spotify-charts-analysis
conda activate spotify-charts-analysis
```

To make all the scripts work out-of-the-box you can simply install the `helpers` package by running
```
pip install -e .
```
Alternatively, you can of course also just install packages one-by-one as you are running into issues trying to execute things lol

## Findings
Track lyrics on Spotify can be incorrect, even for fairly popular songs (for example [this](https://open.spotify.com/track/59mdyQniSaNFeXaKMGu9RB) instrumental track that for some reason has lyrics).

## Open Questions
 - Identifying cause for 404 and 403 HTTP errors when fetching lyrics from the internal API: Initially, I assumed this has something to do with the availability of tracks (i.e. you can only fetch data for tracks that are available in the region of the user associated with the Spotify account used to fetch the data OR if you are making the request from a location where the track is available). This proved to be false: sometimes lyrics could also be fetched for tracks without any available market, or for tracks available in other markets, but not the one of the user.
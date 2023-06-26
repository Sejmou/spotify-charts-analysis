# Spotify Charts Data
In this repo, I collect and explore data related to tracks featured in the [Spotify Charts](https://charts.spotify.com/charts). I build on the work I did in [this repo](https://github.com/Sejmou/spotify-charts-viz.git) where I already did quite a lot of stuff. Unfortunately, the data processing in this project was really messy, so I decided to start from scratch to create a cleaner dataset.

## Data collection scripts
I have come up with CLI scripts for fetching data related to the Spotify Charts. They can be found in the `cli-scripts` subfolder of this project.

### Part 1: Scraping chart data
First, I came up with scripts for assembling data for tracks of the Spotify Daily Top 200. Unfortunately, this data is not available via the API. Furthermore, one has to download chart CSV files for each region and track separately (by navigating to the Spotify Charts page and clicking the download button) which is very inconvenient. 

However, I worked around this by creating a `download_charts.py` that automates that process using `selenium`. Chart data for a given date range and selection of regions can be downloaded automatically to a specified folder.

Afterwards, `combine_charts.py` can be used to combine the data into a single file (the `.parquet` format is preferable compared to `.csv`, however both are supported).

### Part 2: Obtaining metadata for tracks, albums, and artists
I wrote `get_track_metadata.py`, a script for fetching metadata for each track in the charts dataset (or, actually any `parquet` file containing a `track_id` column with Spotify track IDs). This creates several `parquet` files in a specified output directory with metadata concerning the provided tracks. The `metadata.parquet` file created by this script also contains the IDs of the albums/singles that each track was released on.

Metadata for albums can be fetched with get_album_metadata.py`. Just provide a path to a `parquet` file containing Spotify album IDs (note: Spotify refers to both albums and singles as albums, i.e. both are accessible via the `/albums` endpoint in the API). Again, multiple files are created in a specified output directory. The `artists.parquet` file contains the IDs of all artists featured in each track (together with information about who is the primary artist, secondary artists etc.).

Finally, the `get_artist_metadata.py` script gets metadata for all unique artist IDs contained in the `artist_id` columns of specified `.parquet` files. Just as with the other two scripts, data is again written to a specified output directory.

### Part 3: Scraping credits information
Unfortunately, the information for track credits (performers, songwriters, producers) is also [not available via the Spotify API](https://community.spotify.com/t5/Spotify-for-Developers/Getting-credits-on-a-track/td-p/4950934). I again wrote a scraping script to work around that and obtain credits for given track IDs (`get_credits.py`).

While I was at it, I realized that the song page I scraped the credits from also includes track lyrics. So, I wrote a script for that as well (`get_lyrics.py`). BEWARE: song lyrics are subject to copyright, so be careful not to get into legal issues.

### Example: Get Daily Top 200 chart data for 2022
TODO: add missing commands

```
python cli-scripts/get_track_metadata.py -i data/top200_2022/charts.parquet -o data/top200_2022/tracks
```

```
python cli-scripts/get_album_metadata.py -i data/top200_2022/tracks/metadata.parquet -o data/top200_2022/albums
```

```
python cli-scripts/get_artist_metadata.py -i data/top200_2022/tracks/artists.parquet data/top200_2022/albums/artists.parquet -o data/top200_2022/artists
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
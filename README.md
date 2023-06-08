# Spotify Charts Data
In this repo, I collect and explore data related to tracks featured in the [Spotify Charts](https://charts.spotify.com/charts). I build on the work I did in [this repo](https://github.com/Sejmou/spotify-charts-viz.git) where I already did quite a lot of stuff. Unfortunately, the data processing in this project was really messy, so I decided to start from scratch to create a cleaner dataset.

## Data collection/processing scripts (Python)
### Setup
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

Make sure to also download the [Kaggle Spotify Charts dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts), unzip it, and place the contents in the `data` folder.
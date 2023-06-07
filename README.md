# Spotify Charts Data
In this repo, I collect and explore data related to tracks featured in the [Spotify Charts](https://charts.spotify.com/charts). I build on the work I did in [this repo](https://github.com/Sejmou/spotify-charts-viz.git) where I already did quite a lot of stuff. Unfortunately, the data processing in this project was really messy, so I decided to start from scratch to create a cleaner dataset.

## Data collection/processing scripts (Python)
If you use `conda`, you can just use the `environment.yml` file to create a Python environment with all the required packages:
```
conda env create --file=environment.yml
```

To activate the environment, use
```
conda activate spotify-charts-analysis
```

Make sure to also download the [Kaggle Spotify Charts dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts), unzip it, and place the contents in the `data` folder.
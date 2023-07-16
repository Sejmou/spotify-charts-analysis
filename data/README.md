# Data
All data fetched by the cli-scripts should preferably be stored here. The `create_data_path()` function of the `data.py` submodule of the `helpers` package can create paths pointing to this folder, no matter where it is run from.

By default, this folder is gitignored, however I added some datasets used in other parts of this project. 

## `region_names_and_codes.csv` 

This file includes the mappings from region name to region code (used in the URL of each chart in the Spotify Charts). For example, the URL `https://charts.spotify.com/charts/view/regional-global-daily/2023-06-18` points to the 'Global' daily charts for June 18th, 2023 (the region code being `global`), while `https://charts.spotify.com/charts/view/regional-at-daily/2023-06-18` points to Austria's daily charts for the same day (with 'at' being used as the region code).

The names and codes can be extracted from the region select dropdown on a daily chart page. In JS, just use

```js
const names = Array.from(document.querySelectorAll('[role="option"]')).map(el => el.textContent)
const codes = Array.from(document.querySelectorAll('[role="option"]')).map(el => el.id).map(url => url.split('/')[3].split('-')[1])
```

Note that for later stages of the project (e.g. in the `combine_charts.py` CLI script), I replaced the region code `'global'` with the two-letter code `'ww'` to make every region code two characters long.

As of mid-June 2023, data for 73 regions was available (the region select dropdown contained 73 values), hence this file contains 73 mappings.

## `country_codes.csv`

This file was downloaded from [here](https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv). The two-letter ISO codes in this dataset can be used for joins with the regions in the scraped charts data (output of `combine_charts.py`) or any other region data contained in Spotify API responses (e.g. available markets for tracks).

This file can also be joined with population data contained in `country_population.csv` (which is actually not being used atm, but might become useful later).

## `country_population.csv`

This is population data for several countries of the world (from the 60s to today). It was downloaded from the [World Bank](https://data.worldbank.org/indicator/SP.POP.TOTL). The original file name was `API_SP.POP.TOTL_DS2_en_csv_v2_5551506.csv`. Actually, only a subset of the data is used in this project (recent data for countries with chart data on the Spotify Charts website).
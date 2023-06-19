# Data
All data fetched by the cli-scripts should preferably be stored here. The `create_data_path()` function of the `data.py` submodule of the `helpers` package can create paths pointing to this folder, no matter where it is run from.

By default, this folder is gitignored, however I added the `region_codes.txt` file. It includes the mappings from region name to region code (used in the URL of each chart in the Spotify Charts). For example, the URL `https://charts.spotify.com/charts/view/regional-global-daily/2023-06-18` points to the 'Global' daily charts for June 18th, 2023 (the region code being `global`), while `https://charts.spotify.com/charts/view/regional-at-daily/2023-06-18` points to Austria's daily charts for the same day (with 'at' being used as the region code).

The names and codes can be extracted from the region select dropdown on a daily chart page. In JS, just use

```js
const names = Array.from(document.querySelectorAll('[role="option"]')).map(el => el.textContent)
const codes = Array.from(document.querySelectorAll('[role="option"]')).map(el => el.id).map(url => url.split('/')[3].split('-')[1])
```

Note that for later stages of the project (e.g. in the `comine_charts.py` CLI script), I replaced the region code `'global'` with the two-letter code `'ww'` to make every region code two characters long.
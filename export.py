from pathlib import Path

import pandas as pd

ROWS = 500

data_path = Path(__file__).parent / "data"
data_path.mkdir(exist_ok=True)

CoreTracks = pd.read_parquet(data_path / "CoreTracks.parquet").filter(
    items=["ArtistID", "AlbumID", "Title", "Rating"]
)
CoreAlbums = (
    pd.read_parquet(data_path / "CoreAlbums.parquet")
    .filter(items=["AlbumID", "Title"])
    .rename(columns={"Title": "Album"})
)
CoreArtists = (
    pd.read_parquet(data_path / "CoreArtists.parquet")
    .filter(items=["ArtistID", "Name"])
    .rename(columns={"Name": "Artist"})
)


full_df = (
    CoreTracks.merge(CoreAlbums)
    .merge(CoreArtists)
    .filter(items=["Artist", "Album", "Title", "Rating"])
)

for rating in [3, 4, 5]:
    subset_rating = full_df.pipe(lambda df: df[df["Rating"] == rating])
    for part in range(len(subset_rating) // ROWS + 1):
        subset_rows = subset_rating.iloc[ROWS * part : ROWS * (part + 1)]
        subset_rows.to_csv(
            data_path / f"playlist_rating{rating}_part{part}.csv", sep=";"
        )

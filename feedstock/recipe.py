import gcsfs
import apache_beam as beam

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata, ConsolidateDimensionCoordinates, OpenURLWithFSSpec, OpenWithXarray, StoreToZarr
)

gcs = gcsfs.GCSFileSystem()

file_list = gcs.glob("gcp-public-data-goes-16/ABI-L2-DSRF/*/*/*/*.nc")
file_list = ["https://storage.googleapis.com/" + uri for uri in file_list]

pattern = pattern_from_file_sequence(file_list=file_list, concat_dim="t")


def preprocess(index, ds):
    return index, ds.expand_dims(dim="t").sel(lat=slice(31, 22), lon=slice(-100, -72))


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(cache=None)
    | OpenWithXarray()
    | beam.MapTuple(preprocess)
    | StoreToZarr(
        combine_dims=pattern.combine_dim_keys,
        store_name="goes.zarr",
        target_chunks={"t": len(file_list)},
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
)

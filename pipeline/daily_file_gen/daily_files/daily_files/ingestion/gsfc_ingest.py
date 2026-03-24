import logging
import os
from typing import Iterable, TextIO

import numpy as np
import pandas as pd
import xarray as xr

from daily_files.config.paths import REF_FILES_DIR
from daily_files.ingestion.ingest import IngestedData, Ingestor
from utilities.aws_utils import aws_manager


class GSFCIngestor(Ingestor):
    def ingest(self, file_objs: Iterable[TextIO], **kwargs) -> IngestedData:
        bucket = kwargs.get("bucket")

        with [xr.open_dataset(file_obj, engine="h5netcdf") for file_obj in file_objs] as opened_files:
            og_ds = xr.concat(opened_files, dim="N_Records")
            cycles = np.concatenate([np.full_like(ds["ssha"].values, ds.attrs["merged_cycle"]) for ds in opened_files])
              
        # opened_files = [xr.open_dataset(file_obj, engine="h5netcdf") for file_obj in file_objs]
        # cycles = np.concatenate([np.full_like(ds["ssha"].values, ds.attrs["merged_cycle"]) for ds in opened_files])
        # og_ds = xr.concat(opened_files, dim="N_Records")
        # opened_files = []

        ssha = og_ds["ssha"].values / 1000  # Convert from mm
        lats = og_ds["lat"].values
        lons = og_ds["lon"].values
        times = og_ds["time"].values
        # dac = self._compute_dac(np.unique(cycles), ssha, bucket)
        dac, inv_bar_cor = self._compute_dac_and_inv_bar(np.unique(cycles), ssha, bucket)
        cycles, passes = self._compute_cycles_passes(og_ds, cycles)

        return IngestedData(
            ssha=ssha,
            lat=lats,
            lon=lons,
            time=times,
            cycles=cycles,
            passes=passes,
            dac=dac,
            inv_bar_cor=inv_bar_cor,
            source_specific={
                "og_ds": og_ds,
            },
        )

    def _compute_cycles_passes(self, ds: xr.Dataset, cycles: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Computes passes using look up table that converts a reference_orbit and index value to pass number.
        GSFC uses slightly different pass/cycle definitions. We need to increment cycle number in the ascending half
        below the equator of a pass where pass==1
        """
        logging.info("Computing pass values")
        df = pd.read_csv(
            os.path.join(REF_FILES_DIR, "complete_gsfc_pass_lut.csv"),
            converters={"id": str},
        ).set_index("id")

        ds_ids = [
            str(orbit).zfill(3) + str(index).zfill(4)
            for orbit, index in zip(ds["reference_orbit"].values, ds["index"].values)
        ]
        passes = df.loc[ds_ids]["pass"].values

        index_of_wrap = np.where(passes[:-1] > passes[1:])[0][0] + 1
        cycles[index_of_wrap:][(cycles[index_of_wrap:] == cycles[0]) & (passes[index_of_wrap:] == 1)] += 1
        return cycles, passes

    # def _compute_dac(self, unique_cycles: np.ndarray, ssha: np.ndarray, bucket: str) -> np.ndarray:
    #     """
    #     Loads corresponding NOIB cycle file(s) and subtracts "ssha_noib" from our ssha values
    #     """
    #     all_obj_ds = []
    #     noib_bucket_path = f"s3://{bucket}/source_data/GSFC_6.1/GSFC_6.1_NOIB"
    #     try:
    #         for cycle_num in unique_cycles:
    #             logging.info(f"Streaming cycle {cycle_num}")
    #             noib_filename = f"Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_{int(cycle_num):04}.V6_1.nc"
    #             src = os.path.join(noib_bucket_path, noib_filename)
    #             obj = aws_manager.stream_obj(src)
    #             obj_ds = xr.open_dataset(obj, engine="h5netcdf")
    #             all_obj_ds.append(obj_ds)
    #         noib_ds = xr.concat(all_obj_ds, dim="N_Records")
    #         ssha_noib = noib_ds["ssha_noib"].values / 1000
    #     except Exception as e:
    #         logging.error(e)
    #         ssha_noib = np.full_like(ssha, 0)
    #     return ssha_noib - ssha

    def _compute_dac_and_inv_bar(self, unique_cycles: np.ndarray, ssha: np.ndarray, bucket: str) -> tuple[np.ndarray]:
        """
        Loads corresponding NO STATIC IB cycle file(s) and NO DAC cycle file(s) to compute dac and inv_bar_cor
        """
        noib_bucket_path = f"s3://{bucket}/source_data/GSFC_6.1/GSFC_6.1_NO_STATIC_IB"
        nodac_bucket_path = f"s3://{bucket}/source_data/GSFC_6.1/GSFC_6.1_NODAC"

        all_noib_ds = []
        all_nodac_ds = []
        try:
            for cycle_num in unique_cycles:
                logging.info(f"Streaming cycle {cycle_num}")
                filename = f"Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_{int(cycle_num):04}.V6_1.nc"

                noib_src = os.path.join(noib_bucket_path, filename)
                noib_ds = xr.open_dataset(aws_manager.stream_obj(noib_src), engine="h5netcdf")
                all_noib_ds.append(noib_ds)

                nodac_src = os.path.join(nodac_bucket_path, filename)
                nodac_ds = xr.open_dataset(aws_manager.stream_obj(nodac_src), engine="h5netcdf")
                all_nodac_ds.append(nodac_ds)

            noib_ds = xr.concat(all_noib_ds, dim="N_Records")
            ssha_noib = noib_ds["ssha"].values / 1000

            nodac_ds = xr.concat(all_nodac_ds, dim="N_Records")
            ssha_nodac = nodac_ds["ssha"].values / 1000
        except Exception as e:
            logging.error(e)
            ssha_noib = np.full_like(ssha, 0)
            ssha_nodac = np.full_like(ssha, 0)

        dac = ssha_nodac - ssha
        inv_bar_cor = ssha_nodac - ssha_noib

        return dac, inv_bar_cor

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
    def ingest(self, file_objs: Iterable[TextIO], bucket: str | None = None, **kwargs) -> IngestedData:
        if not bucket:
            raise ValueError(
                "GSFCIngestor.ingest requires a non-empty 'bucket' to load the IB_APPLIED and NO_ATMOS flavors"
            )

        opened_files = [xr.open_dataset(file_obj, engine="h5netcdf") for file_obj in file_objs]
        cycles = np.concatenate([np.full_like(ds["ssha"].values, ds.attrs["merged_cycle"]) for ds in opened_files])
        og_ds = xr.concat(opened_files, dim="N_Records")
        opened_files = []

        ssha = og_ds["ssha"].values / 1000  # Convert from mm
        lats = og_ds["lat"].values
        lons = og_ds["lon"].values
        times = og_ds["time"].values
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

    @staticmethod
    def _compute_cycles_passes(ds: xr.Dataset, cycles: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
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

    @staticmethod
    def _compute_dac_and_inv_bar(
        unique_cycles: np.ndarray, ssha: np.ndarray, bucket: str
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Loads the IB-applied and no-atmospheric-correction cycle file(s) to compute dac and inv_bar_cor.

        The main product ``ssha`` has DAC applied; the IB_APPLIED flavor has the inverse
        barometer applied instead; the NO_ATMOS flavor has no atmospheric load correction of
        any kind. Differencing against the NO_ATMOS baseline recovers each correction:
            dac         = ssha_no_atmos - ssha            (NO_ATMOS - REFERENCE)
            inv_bar_cor = ssha_no_atmos - ssha_ib_applied (NO_ATMOS - IB_APPLIED)
        Verified against native S6 dac/inv_bar_cor at the source transition (corr 0.99999,
        sub-mm residual).
        """
        ib_bucket_path = f"s3://{bucket}/source_data/GSFC_6.1/GSFC_6.1_IB_APPLIED"
        no_atmos_bucket_path = f"s3://{bucket}/source_data/GSFC_6.1/GSFC_6.1_NO_ATMOS"

        all_ib_ds = []
        all_no_atmos_ds = []
        for cycle_num in unique_cycles:
            logging.info(f"Streaming cycle {cycle_num}")
            filename = f"Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_{int(cycle_num):04}.V6_1.nc"

            ib_src = os.path.join(ib_bucket_path, filename)
            ib_ds = xr.open_dataset(aws_manager.stream_obj(ib_src), engine="h5netcdf")
            all_ib_ds.append(ib_ds)

            no_atmos_src = os.path.join(no_atmos_bucket_path, filename)
            no_atmos_ds = xr.open_dataset(aws_manager.stream_obj(no_atmos_src), engine="h5netcdf")
            all_no_atmos_ds.append(no_atmos_ds)

        ib_ds = xr.concat(all_ib_ds, dim="N_Records")
        ssha_ib_applied = ib_ds["ssha"].values / 1000

        no_atmos_ds = xr.concat(all_no_atmos_ds, dim="N_Records")
        ssha_no_atmos = no_atmos_ds["ssha"].values / 1000

        # dac/inv_bar_cor are element-wise differences across independently loaded sources
        # (input files vs. S3 flavors), so the records must align 1:1. Guard against a
        # count mismatch (missing/extra cycle) rather than silently emitting garbage.
        if ssha_ib_applied.shape != ssha.shape or ssha_no_atmos.shape != ssha.shape:
            raise ValueError(
                "GSFC flavor record-count mismatch: "
                f"ssha={ssha.shape}, no_atmos={ssha_no_atmos.shape}, ib_applied={ssha_ib_applied.shape}"
            )

        dac = ssha_no_atmos - ssha
        inv_bar_cor = ssha_no_atmos - ssha_ib_applied

        return dac, inv_bar_cor

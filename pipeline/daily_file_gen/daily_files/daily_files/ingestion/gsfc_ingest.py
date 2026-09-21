import logging
import os
from contextlib import ExitStack
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

        with ExitStack() as stack:
            opened = [stack.enter_context(xr.open_dataset(fo, engine="h5netcdf")) for fo in file_objs]

            # Per-record cycle id from each file's merged_cycle attr. sizes/dtype are metadata,
            # so this doesn't read ssha (which is read once, below, from the concatenation).
            cycles = np.concatenate(
                [
                    np.full(ds.sizes["N_Records"], ds.attrs["merged_cycle"], dtype=ds["ssha"].dtype)
                    for ds in opened
                ]
            )

            combined = xr.concat(opened, dim="N_Records")
            ssha = combined["ssha"].values / 1000  # Convert from mm
            lats = combined["lat"].values
            lons = combined["lon"].values
            times = combined["time"].values
            reference_orbit = combined["reference_orbit"].values
            index = combined["index"].values

            # og_ds is carried downstream solely to read flag (values + flag_meanings attr)
            # and Surface_Type; keeping it an xr.Dataset preserves those attrs.
            og_ds = combined[["flag", "Surface_Type"]].load()

        dac, inv_bar_cor = self._compute_dac_and_inv_bar(np.unique(cycles), ssha, bucket)
        cycles, passes = self._compute_cycles_passes(reference_orbit, index, cycles)

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
    def _compute_cycles_passes(
        reference_orbit: np.ndarray, index: np.ndarray, cycles: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
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
            str(orbit).zfill(3) + str(idx).zfill(4)
            for orbit, idx in zip(reference_orbit, index)
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
        try:
            for cycle_num in unique_cycles:
                logging.info(f"Streaming cycle {cycle_num}")
                filename = f"Merged_TOPEX_Jason_OSTM_Jason-3_Sentinel-6_Cycle_{int(cycle_num):04}.V6_1.nc"

                ib_src = os.path.join(ib_bucket_path, filename)
                all_ib_ds.append(xr.open_dataset(aws_manager.stream_obj(ib_src), engine="h5netcdf"))

                no_atmos_src = os.path.join(no_atmos_bucket_path, filename)
                all_no_atmos_ds.append(xr.open_dataset(aws_manager.stream_obj(no_atmos_src), engine="h5netcdf"))

            # .values materializes standalone numpy copies, so the datasets can be closed afterward.
            ssha_ib_applied = xr.concat(all_ib_ds, dim="N_Records")["ssha"].values / 1000
            ssha_no_atmos = xr.concat(all_no_atmos_ds, dim="N_Records")["ssha"].values / 1000
        finally:
            for ds in (*all_ib_ds, *all_no_atmos_ds):
                ds.close()

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

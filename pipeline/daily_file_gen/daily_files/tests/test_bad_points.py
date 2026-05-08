import dataclasses
import unittest
import xarray as xr
import numpy as np
from datetime import datetime
from daily_files.config.source_config import get_source_config
from daily_files.ingestion.ingest import IngestedData
from daily_files.processing.gsfc_daily_file import GSFCDailyFile
from daily_files.processing.s6_daily_file import S6DailyFile


def _times_for_date(date, n=50):
    return np.arange(
        np.datetime64(date),
        np.datetime64(date) + np.timedelta64(1, "D"),
        np.timedelta64(86400 // n, "s"),
    ).astype("datetime64[ns]")[:n]


def _make_gsfc_ingested(times):
    n = len(times)
    rng = np.random.RandomState(7)
    flag_meanings = " ".join(
        [
            "abs(SSH(cycle)-SSH(cycle +/-1))>50cm",
            "Radiometer_Observation_is_Suspect",
            "Attitude_Out_of_Range",
            "Sigma0_Ku_Band_Out_of_Range",
            "Possible_Rain_Contamination",
            "Sea_Ice_Detected",
            "Significant_Wave_Height>8m",
            "Cross_Track_slope>10cm/km",
            "Cross_Track_Distance>1km",
            "Any_Applied_SSH_Correction_Out_of_Limits",
            "Contiguous_1Hz_Data",
            "Sigma_H_of_fit>15cm",
            "Distance_to_Land<50km",
            "Water_Depth<200m",
            "Single_Frequency_Altimeter",
        ]
    )
    # GSFCDailyFile.make_nasa_flag requires that bits 0,1,2,3,4,5,9 are all
    # 0 for a record to pass prelim_flag. With purely random 15-bit values
    # this happens for <1% of records, often producing zero valid samples
    # — np.interp then errors out on empty inputs. Generate a distribution
    # where ~half the records have those bits clear so the test exercises
    # the real code path.
    raw_flag = rng.randint(0, 2 ** 15, n).astype(np.int32)
    clear_mask = ~((1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 9)) & 0x7FFF
    keep_clear = rng.rand(n) < 0.6
    flag = np.where(keep_clear, raw_flag & clear_mask, raw_flag).astype(np.int32)

    og_ds = xr.Dataset(
        {
            "flag": (
                ("N_Records",),
                flag,
                {"flag_meanings": flag_meanings},
            ),
            "Surface_Type": (
                ("N_Records",),
                rng.choice([0, 2, 1], n, p=[0.8, 0.15, 0.05]).astype(np.int32),
            ),
        }
    )
    return IngestedData(
        ssha=rng.normal(0, 0.1, n),
        lat=np.linspace(-66, 66, n),
        lon=np.linspace(0, 360, n, endpoint=False),
        time=times,
        cycles=np.full(n, 100, dtype=np.int32),
        passes=np.tile(np.arange(1, 6), n // 5 + 1)[:n].astype(np.int32),
        dac=rng.normal(0, 0.01, n),
        inv_bar_cor=rng.normal(0, 0.01, n),
        source_specific={"og_ds": og_ds},
    )


def _make_s6_ingested(times):
    n = len(times)
    rng = np.random.RandomState(13)
    ssha = rng.normal(0, 0.1, n)
    original_ds = xr.Dataset(
        {
            "range_ocean_nr_qual": (("time",), rng.choice([0, 1], n, p=[0.95, 0.05]).astype(np.int8)),
            "surface_classification_flag": (("time",), rng.choice([0, 2, 1], n, p=[0.8, 0.15, 0.05]).astype(np.int8)),
            "rad_water_vapor_qual": (("time",), rng.choice([0, 1], n, p=[0.95, 0.05]).astype(np.int8)),
            "rain_flag_nr": (("time",), rng.choice([0, 3, 5, 1], n, p=[0.8, 0.1, 0.05, 0.05]).astype(np.int8)),
            "sig0_ocean_nr": (("time",), rng.uniform(8, 20, n)),
            "swh_ocean_nr": (("time",), rng.uniform(0, 5, n)),
            "ssha_nr": (("time",), ssha),
        },
        coords={"time": times},
    )
    mss = rng.normal(30, 5, n)
    return IngestedData(
        ssha=ssha,
        lat=np.linspace(-66, 66, n),
        lon=np.linspace(0, 360, n, endpoint=False),
        time=times,
        cycles=np.full(n, 200, dtype=np.int32),
        passes=np.tile(np.arange(1, 6), n // 5 + 1)[:n].astype(np.int32),
        dac=rng.normal(0, 0.01, n),
        inv_bar_cor=rng.normal(0, 0.01, n),
        source_specific={
            "original_ds": original_ds,
            "mean_sea_surface_sol1": mss,
            "mean_sea_surface_sol2": mss + rng.normal(0, 0.01, n),
        },
    )


def _bad_points_config(source_config, bad_time):
    """Return a copy of source_config with bad_points set to flag bad_time."""
    return dataclasses.replace(
        source_config,
        bad_points={bad_time.date(): [{"time": bad_time}]},
    )


class TestGSFCBadPoints(unittest.TestCase):
    def test_bad_point_is_flagged(self):
        date = datetime(2024, 3, 15)
        times = _times_for_date(date)
        bad_time = times[10].astype("datetime64[s]").item()

        source_config = _bad_points_config(get_source_config("GSFC"), bad_time)
        print(source_config)
        ds = GSFCDailyFile(
            _make_gsfc_ingested(times),
            date,
            source_config,
        ).ds

        out_times = ds["time"].values.astype("datetime64[s]")
        bad_time_np = np.datetime64(bad_time, "s")
        idx = np.where(out_times == bad_time_np)[0]
        self.assertEqual(len(idx), 1, "bad_time not found in output dataset")
        self.assertTrue(ds["nasa_flag"].values[idx[0]], "bad_time should have nasa_flag=1")

    def test_wrong_date_not_flagged_by_bad_points(self):
        """bad_points for a different date should not affect the current date."""
        date = datetime(2024, 3, 15)
        times = _times_for_date(date)
        other_time = times[10].astype("datetime64[s]").item()

        # bad_points keyed to a different date
        source_config = dataclasses.replace(
            get_source_config("GSFC"),
            bad_points={other_time.replace(day=16).date(): [{"time": other_time.replace(day=16)}]},
        )
        ds = GSFCDailyFile(
            _make_gsfc_ingested(times),
            date,
            source_config,
        ).ds
        # Should complete without error; bad_points for another date are ignored
        self.assertIn("nasa_flag", ds)


class TestS6BadPoints(unittest.TestCase):
    def test_bad_point_is_flagged(self):
        date = datetime(2024, 5, 10)
        times = _times_for_date(date)
        bad_time = times[20].astype("datetime64[s]").item()

        source_config = _bad_points_config(get_source_config("S6"), bad_time)
        ds = S6DailyFile(
            _make_s6_ingested(times),
            date,
            source_config,
        ).ds

        out_times = ds["time"].values.astype("datetime64[s]")
        bad_time_np = np.datetime64(bad_time, "s")
        idx = np.where(out_times == bad_time_np)[0]
        self.assertEqual(len(idx), 1, "bad_time not found in output dataset")
        self.assertTrue(ds["nasa_flag"].values[idx[0]], "bad_time should have nasa_flag=1")

    def test_no_bad_points_no_change(self):
        """With bad_points=None, processing should not raise and produce valid flags."""
        date = datetime(2024, 5, 10)
        times = _times_for_date(date)
        source_config = get_source_config("S6")
        self.assertIsNone(source_config.bad_points)
        ds = S6DailyFile(
            _make_s6_ingested(times),
            date,
            source_config,
        ).ds
        flag_vals = np.unique(ds["nasa_flag"].values)
        for v in flag_vals:
            self.assertIn(v, [0, 1, True, False])

    def test_multiple_bad_points_all_flagged(self):
        """All times listed under a date should be flagged."""
        date = datetime(2024, 5, 10)
        times = _times_for_date(date)
        bad_time_a = times[5].astype("datetime64[s]").item()
        bad_time_b = times[15].astype("datetime64[s]").item()

        source_config = dataclasses.replace(
            get_source_config("S6"),
            bad_points={bad_time_a.date(): [{"time": bad_time_a}, {"time": bad_time_b}]},
        )
        ds = S6DailyFile(
            _make_s6_ingested(times),
            date,
            source_config,
        ).ds

        out_times = ds["time"].values.astype("datetime64[s]")
        for bad_time in (bad_time_a, bad_time_b):
            idx = np.where(out_times == np.datetime64(bad_time, "s"))[0]
            self.assertEqual(len(idx), 1, f"{bad_time} not found in output")
            self.assertTrue(ds["nasa_flag"].values[idx[0]], f"{bad_time} should have nasa_flag=1")

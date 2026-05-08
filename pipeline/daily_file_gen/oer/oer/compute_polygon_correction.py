import numpy as np
from datetime import datetime, timezone
import xarray as xr
import logging
from scipy.interpolate import PPoly

from oer.oerfit import oerfit
from utilities.source_profile import daily_filename_prefix


def create_polygon(xover_ds: xr.Dataset, date: datetime, source: str) -> xr.Dataset:
    """Fit a cubic-spline orbit-error polygon to self-crossover differences.

    Extracts SSH crossover pairs from *xover_ds*, filters to the target day
    (with a 2-hour margin), and fits a piecewise cubic spline via ``oerfit``.
    Returns a dataset containing spline coefficients, breakpoints, and
    per-interval diagnostics.  If no crossover data falls within the day
    window, all coefficients are set to zero.
    """
    logging.info("Creating polygon")

    pgon_def_duration = 86400  # define polygon over 1 day
    pgon_t_margin = 7200  # keep 2 hours of extra data to avoid jumps between days
    ssh_max_error = 0.3  # ignore absolute xover differences larger than this in meters

    ref_timestamp = datetime(1990, 1, 1, tzinfo=timezone.utc).timestamp()
    cur_timestamp = date.replace(tzinfo=timezone.utc).timestamp()

    psec1 = np.float64(xover_ds["time1"].values + ref_timestamp)
    psec2 = np.float64(xover_ds["time2"].values + ref_timestamp)
    cycle1 = xover_ds["cycle1"].values
    cycle2 = xover_ds["cycle2"].values
    pass1 = xover_ds["pass1"].values
    pass2 = xover_ds["pass2"].values
    ssh1 = xover_ds["ssh1"].values
    ssh2 = xover_ds["ssh2"].values

    # compute trackid from passnum & cyc
    trackid1 = cycle1 * 10000 + pass1
    trackid2 = cycle2 * 10000 + pass2

    # since these are self crossovers, stack pass1 & pass2 data
    dssh0 = ssh1 - ssh2
    dssh = np.concatenate((dssh0, -dssh0))
    psec = np.concatenate((psec1, psec2))
    trackid = np.concatenate((trackid1, trackid2))

    # need to make a time variable in units of hours, refernced
    # to current date at time 00:00:00.  Polygon will be expressed
    # in terms of this variable
    phours = (psec - cur_timestamp) / 3600

    # pick times for spline fit: first find all passes with data
    # within 2 hour window before & after the current day.
    iilimit = np.where(
        (psec >= cur_timestamp - pgon_t_margin) & (psec < cur_timestamp + pgon_def_duration + pgon_t_margin)
    )[0]

    # case of no data in this day, make a polynomial that's all zeros
    if len(iilimit) == 0:
        logging.warning(f"No data for {source} {date}")
        tbrk = np.array(range(-3, 28))
        coef = np.zeros((4, len(tbrk) - 1))
        rms_sig = np.zeros(len(tbrk) - 1)
        rms_res = np.zeros(len(tbrk) - 1)
        nint = np.zeros(len(tbrk) - 1)
    else:
        # make list of passes in this time window & find min/max times for them
        cplist, iitrack = np.unique(trackid[iilimit], return_index=True)
        mintime = min(phours[trackid == np.min(cplist)])
        maxtime = max(phours[trackid == np.max(cplist)])

        # find list of data in this time window & abs(dssh) < ssh_max_error
        iitoday = np.where((phours >= mintime) & (phours <= maxtime) & (np.abs(dssh) < ssh_max_error))[0]

        # sort index by time
        ii = np.argsort(phours[iitoday])
        iitoday = iitoday[ii]

        # padd with zeros to make sure polygon doesn't blow up away from data
        hpmin = np.arange(-5, mintime, 0.3)
        hpmax = np.arange(maxtime, 29, 0.3)
        phours_pad = np.concatenate((hpmin, phours[iitoday], hpmax))
        dssh_pad = np.concatenate((np.zeros_like(hpmin), dssh[iitoday], np.zeros_like(hpmax)))
        trackid_pad = np.concatenate(
            (
                np.full_like(hpmin, trackid[iitoday[0]]),
                trackid[iitoday],
                np.full_like(hpmax, trackid[iitoday[-1]]),
            )
        )

        # send to our own spline function for fit
        coef, tbrk, rms_sig, rms_res, nint = oerfit(phours_pad, dssh_pad, trackid_pad)

    # create xarray data set of variable to save in netcdf file
    ds = xr.Dataset(
        data_vars={
            "coef": (
                ["N_order", "N_intervals"],
                coef,
                {
                    "units": "meters/hour^3, meters/hour^2, meters/hour, meters",
                    "long_name": "coefficients for cubic spine polynomials",
                },
            ),
            "tbrk": (
                ["N_breaks"],
                tbrk,
                {
                    "units": "Hours since "
                    + datetime.strftime(date.replace(tzinfo=timezone.utc), "%Y-%m-%d %H:%M:%S %Z"),
                    "long_name": "Breaks in cubic spline",
                },
            ),
            "rms_sig": (
                ["N_intervals"],
                rms_sig,
                {
                    "units": "m",
                    "long_name": "RMS of crossover difference pairs in each interval",
                },
            ),
            "rms_res": (
                ["N_intervals"],
                rms_res,
                {
                    "units": "m",
                    "long_name": "RMS of residuals after cubic spline is removed",
                },
            ),
            "nint": (
                ["N_intervals"],
                nint,
                {
                    "units": "counts",
                    "long_name": "Number of crossovers in each interval",
                },
            ),
        },
        attrs={
            "title": f"{source} Spline coefficents for Orbit Error Reduction",
            "subtitle": f"created for {source} {date}",
        },
    )
    return ds


def evaluate_correction(polygon_ds: xr.Dataset, daily_file_ds: xr.Dataset, date: datetime, source: str) -> xr.Dataset:
    """Evaluate the spline polygon at each daily-file time step.

    Returns a dataset with a single ``oer`` variable representing the
    additive orbit-error correction (add to SSH to reduce orbit error).
    """
    logging.info("Evaluating correction")

    # load coefs and tbreaks from polynomial file
    pp = PPoly(polygon_ds["coef"].values, polygon_ds["tbrk"].values)

    # compute hours since start of this day
    ssh_time = daily_file_ds["time"].values
    hours_since_start = (ssh_time - np.datetime64(date)).astype("timedelta64[s]").astype(int) / 3600

    # compute orbit error reduction, as additive correction to ssh
    oer = -1 * pp(hours_since_start)

    # create xarray data set of variable to save in netcdf file
    ds = xr.Dataset(
        data_vars={
            "oer": (
                ["time"],
                oer,
                {
                    "units": "m",
                    "long_name": "Orbit error reduction",
                    "comment": "Add this variable to ssh and ssh_smoothed to reduce orbit error",
                    "coverage_content_type": "auxiliaryInformation",
                    "valid_min": -1e100,
                    "valid_max": 1e100,
                },
            )
        },
        coords={"time": ("time", daily_file_ds["time"].data, daily_file_ds["time"].attrs)},
        attrs={
            "title": f"{source} Orbit Error Reduction, interpolated onto ssh",
            "subtitle": f"created for {daily_filename_prefix(source)}_{date.strftime('%Y%m%d')}.nc",
        },
    )
    ds["time"].encoding["units"] = daily_file_ds["time"].encoding["units"]
    return ds


def apply_correction(daily_file_ds: xr.Dataset, correction_ds: xr.Dataset) -> xr.Dataset:
    """Apply the OER correction to a daily file dataset (in-place).

    Adds the ``oer`` variable to ``ssha`` and ``ssha_smoothed``, copies the
    ``oer`` variable into the daily file, and sets ``product_generation_step``
    to ``"2"``.  Raises ``ValueError`` if the time dimensions do not match.
    """
    logging.info("Applying correction")

    if len(correction_ds["time"]) != len(daily_file_ds["time"]):
        raise ValueError("Unable to apply correction. Differing sizes between correction and daily file.")

    daily_file_ds["oer"] = (("time"), correction_ds["oer"].values)
    daily_file_ds["oer"].attrs = {
        "units": "m",
        "long_name": "Orbit error reduction",
        "comment": "Add this variable to ssh and ssh_smoothed to reduce orbit error",
        "coverage_content_type": "auxiliaryInformation",
        "valid_min": -1.0e100,
        "valid_max": 1.0e100,
    }

    if len(daily_file_ds["time"]) > 0:
        daily_file_ds["ssha"].values += correction_ds["oer"].values
        daily_file_ds["ssha_smoothed"].values += correction_ds["oer"].values

    daily_file_ds["ssha"].attrs["orbit_error_correction"] = "oer variable added to reduce orbit error"
    daily_file_ds["ssha_smoothed"].attrs["orbit_error_correction"] = "oer variable added to reduce orbit error"

    daily_file_ds.attrs["product_generation_step"] = "2"
    daily_file_ds.attrs["history"] = f"Created on {datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}"

    return daily_file_ds

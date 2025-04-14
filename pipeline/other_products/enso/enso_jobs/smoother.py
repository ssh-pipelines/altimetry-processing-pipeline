# -*- coding: utf-8 -*-
"""
test_diff_smooth.py - python script to load up diffusion and use it to smooth
an ssh map
Created on Tue Apr 16 2024
@author: jwillis
"""

import numpy as np
import netCDF4 as nc
import xarray as xr
from scipy.sparse import csr_matrix, lil_matrix


def new_smoother(ssh: np.ndarray, y: np.ndarray, x: np.ndarray):
    # load data from diffusion operator file
    with nc.Dataset(
        "enso_jobs/ref_files/diff_operator_halfdeg.nc", "r", format="NETCDF4"
    ) as f:
        ddata = np.array(f["ddata"][:])
        dindices = np.array(f["dindices"][:])
        dindptr = np.array(f["dindptr"][:])

        # load grid cell location indicies
        cent = np.array(f["cent"][:])
        west = np.array(f["west"][:])
        east = np.array(f["east"][:])
        south = np.array(f["south"][:])
        north = np.array(f["north"][:])

        # also get grid variables just in case
        blon = np.array(f["blon"][:])
        blat = np.array(f["blat"][:])
        bmask = np.array(f["bmask"][:])
        Nt = np.array(f["Nt"][:])[0]
        N = f.dimensions["Ngrid"].size

    # create diffusion matrix
    diffmat = csr_matrix((ddata, dindices, dindptr), shape=(N, N))

    # load cloud grid to work on
    nx = len(x)
    ny = len(y)

    # first make sure we are using the same grid
    if not ((len(x) == len(blon)) & (len(y) == len(blat))):
        raise ValueError("Inconsistent grid sizes")
    if not (min(x) == min(blon)):
        raise ValueError("longitude starting value inconsistent")

    ############################## Removing NaNs ############################
    # need to get rid of NaNs in non-land points, use diffusion operator to do
    # this instead of scipy.griddata (since I HATE scipy)
    # define  operator that  adds all nearest neighbors (in connected basins)
    extrapmat = lil_matrix(diffmat)  # change matrix types twice b/c scipy sucks
    ll = np.arange(N)
    extrapmat[ll, ll] = 0
    extrapmat = csr_matrix(extrapmat)  # change matrix types twice b/c scipy sucks
    # extrapmat will now fill in missing data, using local operator

    # unwrap ssh to use matrix operator
    ssh = ssh.ravel()

    # make sshfill as a copy of ssh where fill in nans
    sshfill = ssh.copy()

    # while loops always make me nervous, so uncomment "print" statement below
    # to make sure you are elminating all possible nans and not stuck in an
    # endless loop
    while len(ll) > 0:
        # first, find the nans that are not on land & not in lakes
        # AND also have a non-nan neighbor
        ii = np.where(np.isnan(sshfill) & (bmask > 0) & (bmask < 1000))[0]
        # subset these to only the ones that have a neighber who is non-nan
        jj = np.where(
            ~np.isnan(sshfill[south[ii]])
            | ~np.isnan(sshfill[north[ii]])
            | ~np.isnan(sshfill[west[ii]])
            | ~np.isnan(sshfill[east[ii]])
        )[0]
        ii = ii[jj]

        # make a set of indices including all of these AND their neighbors
        jj = np.unique(
            np.concatenate((cent[ii], west[ii], east[ii], south[ii], north[ii]))
        )

        # copy sshfill and change nans into zeros
        sshtmp = sshfill[jj].copy()
        # make a vector that is ones where sshtmp is ~nan, and 0 otherwise
        # so we can normalize the sum
        omat = np.ones_like(sshtmp)
        omat[np.isnan(sshtmp)] = 0
        sshtmp[np.isnan(sshtmp)] = 0

        # subset extrapmat to only rows and columns we need
        # do rows first, since matrix is CSR
        E1 = extrapmat[ii, :]
        E2 = E1[:, jj]

        # multiply operator by data
        E = E2.dot(sshtmp)
        # multiply operator by ones, so we can normalize
        En = E2.dot(omat)

        # boundary conditions mean, we can still get En = 0, so ignore these
        ll = np.where(En != 0)[0]

        sshfill[ii[ll]] = E[ll] / En[ll]

    smssh = sshfill.copy()

    for _ in range(Nt - 8):
        smssh = smssh + diffmat.dot(smssh)

    return xr.DataArray(
        data=smssh.reshape((ny, nx)),
        coords={"latitude": y, "longitude": x},
        dims=("latitude", "longitude"),
        name="ssha",
    )

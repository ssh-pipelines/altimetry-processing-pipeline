# -*- coding: utf-8 -*-
"""
oerfit.py - function to fit polynomial to crossovers so that orbit error can
be estimated and reduced, written by J. Willis
28 Feb 2024
"""

from typing import Tuple
import numpy as np

OERFIT_RESULT = Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]


def oerfit(ptime: np.ndarray, dssh: np.ndarray, trackid: np.ndarray) -> OERFIT_RESULT:
    """
        OERFIT computes cubic spline polynomial coefficients based on crossover
    differences.  Either 1, 2 or 3 knots are placed on each pass following
    the algoritm described by La Traon and Ogor, JGR, 1998. In addition, for
    data gaps in time greater than 3/4 hour (over half a pass), a constraint
    is placed on the spline to ensure 0 value, and 0 first derivative in the
    middle of the gap (or a half our from begging and end if gap is more than
    1.2 hours. The same constraint is added 1 hour before the earilest
    datapoint and 1 hour after the last data point.  This ensures that the
    polynomial does not blow up far from the data.  Weighting is assumed to
    be uniform for all input data.


        !!!NOTE!!!!: Time must be provided in HOURS, relative to the 0 hour
    of the earliest day including data!!!


        Usage:  coef,tbrk = oerfit(ptime,dssh,trackid)

                   OERFIT computes cubic spline polynomial coeffiecents from
           self-crossover differences and returns a 3 element tuple
           that can be passed to splev in order to compute the value
           of the spline at an arbitrary time.  NOTE that time MUST
           be given as floating point HOURS since start of earliest
           day in data.
        Args:
                   PTIME - N x 1 array containing time of crossover for "current"
                   pass, in hours since start of current day
                   DSSH - N x 1 difference in ssh between current pass and comparison
                   pass.
                   TRACKID - value representing the cycle and pass number of the
                   "current" pass, comptued as 10000*cycle + pass_number
          Returns:
                   COEF - M x 4 element array containing the coefficients for the
                   spline functions at breaks tbrk.
           TBRK - M x 1 element array of breaks in spline
           RMS_SIG -
           RMS_RES -
           NINT -
    """

    # Validate dimensions of input
    if np.size(ptime, axis=0) != np.size(dssh, axis=0):
        raise ValueError("Input arrays ptime and dssh should be the same size")
    if np.size(dssh, axis=0) != np.size(trackid, axis=0):
        raise ValueError("Input arrays dssh and trackid should be the same size")

    # first, lets make sure we are sorted by time
    ii = np.argsort(ptime)
    pt = ptime[ii]
    ds = dssh[ii]
    tid = trackid[ii]

    # make list of passes in this time window
    cpl = np.unique(tid)

    # need to set breaks, or knots, for our polygon fit
    # start with one at beginning, middle & end of each pass
    tbrk1 = np.zeros(len(cpl)) * np.nan
    tbrk2 = np.zeros(len(cpl)) * np.nan
    tbrk3 = np.zeros(len(cpl)) * np.nan
    # sort number of data points between breaks
    nxo = np.zeros(len(cpl))

    # set breaks based on pass times
    for j in range(len(cpl)):
        ii = np.where(tid == cpl[j])[0]
        nxo[j] = len(ii)
        tbrk1[j] = np.min(pt[ii])
        tbrk2[j] = np.max(pt[ii])
        if (len(ii) > 1) & (len(ii) % 2 == 0):
            ii = ii[1:]
        tbrk3[j] = np.median(pt[ii])

    # add a break for knots separated by more than 10,000 km
    # estiamte speed as 5.7 km/s
    delt = (tbrk2 - tbrk1) * 3600 * 5.7 / 10000
    jj = np.where((delt < 0.5) | (nxo < 10))[0]
    ii = np.where((delt > 1) & (nxo > 20))[0]
    tbrk3 = tbrk3[ii]
    tbrk2 = np.delete(tbrk2, jj)

    # combine all knots into one list
    tbrk = np.append(np.append(tbrk1, tbrk2), tbrk3)
    ii = np.where(~(np.isnan(tbrk)))[0]
    # make a break an hour before and after all data
    tbrk = np.append(np.min(tbrk) - 1, tbrk)
    tbrk = np.append(tbrk, np.max(tbrk) + 1)
    tbrk = np.sort(tbrk)

    # need to add constraints for data gaps > 3/4 hour
    gaps = np.diff(pt)
    smallgapind = np.where((gaps > 0.75) & (gaps < 1.2))[0]
    biggapind = np.where(gaps >= 1.2)[0]
    gapcnst = np.append(pt[smallgapind] + 0.5 * gaps[smallgapind], pt[biggapind] - 0.5)
    gapcnst = np.append(gapcnst, pt[biggapind] + 0.5)

    # compute size of Cmat and Gmat and initialize
    crows = (np.shape(tbrk)[0] - 1) * 3 + np.shape(gapcnst)[0] * 2 + 2
    ccols = (np.shape(tbrk)[0] - 1) * 4
    grows = np.shape(pt)[0]
    gcols = (np.shape(tbrk)[0] - 1) * 4

    cmat = np.zeros((crows, ccols))
    gmat = np.zeros((grows, gcols))
    d = np.zeros(0)

    # fill matricies
    ncst = 0
    gend = 0
    for j in range(len(tbrk) - 1):
        # constrain polygon at tbrk[j+1] (between this and next interval)
        h = tbrk[j + 1] - tbrk[j]
        # constrain continuity
        cst1 = np.array([h**3, h**2, h, 1, 0, 0, 0, -1])
        # continuity of first derivative
        cst1 = np.vstack((cst1, [3 * h**2, 2 * h, 1, 0, 0, 0, -1, 0]))
        # continuity of second derivative
        cst1 = np.vstack((cst1, [6 * h, 2, 0, 0, 0, -2, 0, 0]))

        ii = np.array(range(np.shape(cst1)[0]))
        jj = np.array(range(np.shape(cst1)[1]))

        # put constraints into Cmat
        if j < (len(tbrk) - 2):
            cmat[np.ix_(ii + ncst, jj + j * 4)] = cst1
            ncst = ncst + np.shape(cst1)[0]
        else:
            cmat[np.ix_(ii + ncst, jj[0:4] + j * 4)] = cst1[:, 0:4]
            cmat[np.ix_(ii + ncst, jj[0:4])] = cst1[:, 4:8]
            ncst = ncst + np.shape(cst1)[0]

        # find data in this interval to make Gmat and data vector
        if j == 0:
            iii = np.where((pt >= tbrk[j]) & (pt <= tbrk[j + 1]))[0]
        else:
            iii = np.where((pt > tbrk[j]) & (pt <= tbrk[j + 1]))[0]
        d = np.append(d, ds[iii])

        # fill matrix Gmat with data times in this interval
        if len(iii) > 0:
            t1 = pt[iii] - tbrk[j]
            g1 = t1**3
            g1 = np.vstack((g1, t1**2))
            g1 = np.vstack((g1, t1))
            g1 = np.vstack((g1, t1 * 0 + 1))
            ll = np.array(range(len(iii)))
            nn = np.array(range(4))
            gmat[np.ix_(ll + gend, nn + (j * 4))] = g1.transpose()
            gend = gend + len(iii)

    # add constraints for big data gaps
    for j in range(len(gapcnst)):
        # get time for this constraint
        tconst = gapcnst[j]
        # find index for this break
        tcind = np.floor(np.interp(tconst, tbrk, np.array(range(len(tbrk)))))
        tcind = tcind.astype(int)

        # make constraint
        h = tconst - tbrk[tcind]
        gcst1 = [h**3, h**2, h, 1]
        gcst2 = [3 * h**2, 2 * h, 1, 0]
        cmat[ncst, np.array([0, 1, 2, 3]) + (tcind * 4)] = gcst1
        ncst = ncst + 1
        cmat[ncst, np.array([0, 1, 2, 3]) + (tcind * 4)] = gcst2
        ncst = ncst + 1

    # add constraint to make last break zero & zero slope
    h = tbrk[-1] - tbrk[-2]
    gcst1 = [h**3, h**2, h, 1]
    gcst2 = [3 * h**2, 2 * h, 1, 0]
    cmat[ncst, [-4, -3, -2, -1]] = gcst1
    ncst = ncst + 1
    cmat[ncst, [-4, -3, -2, -1]] = gcst2
    ncst = ncst + 1

    # need to add some room for extra constraints
    npadd = 2 * len(gapcnst) + 2
    fsize = npadd + 3 * (len(tbrk) - 1)
    # fillmat = np.zeros((fsize, fsize))
    filldat = np.zeros((fsize))

    # make least squares matricies
    Atop = np.hstack((gmat.T @ gmat, cmat.T))
    Abot = np.hstack((cmat, np.zeros((fsize, fsize))))
    A = np.vstack((Atop, Abot))
    b = np.append(gmat.T @ d, filldat)

    # solve equations to find coefficients
    coef = np.linalg.solve(A, b)
    # only keep coefs, toss Lagrange multipliers
    nc = len(tbrk) - 1
    coef = coef[range(nc * 4)]

    # use coefficients and data matrix to make residuals
    res = d - (gmat @ coef)

    # do another loop through the polynomial breaks and keep some
    # stats on rms and number of data points that go into each break
    nint = np.zeros(len(tbrk) - 1)
    rms_sig = np.zeros(len(tbrk) - 1)
    rms_res = np.zeros(len(tbrk) - 1)
    for j in range(len(tbrk) - 1):
        if j < len(tbrk) - 2:
            ii = np.where((pt >= tbrk[j]) & (pt < tbrk[j + 1]))[0]
        else:
            ii = np.where((pt >= tbrk[j]) & (pt <= tbrk[j + 1]))[0]
        nint[j] = len(ii)
        if len(ii) > 0:
            rms_sig[j] = np.sqrt(np.mean(d[ii] ** 2))
            rms_res[j] = np.sqrt(np.mean(res[ii] ** 2))

    # reshape coef for return
    coef = coef.reshape((nc, 4)).T
    # return coefs and breaks so polynomial can be constructed
    return coef, tbrk, rms_sig, rms_res, nint

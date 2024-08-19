"""
Crossover detection function, written by J. Willis based on a matlab 
script, xover.m
adapted 24 Feb 2024 to take ssh and time variables along with coordinates
and return time and ssh at crossover location for both passes as well.
23 Dec 2023
"""

from typing import Iterable, List, Tuple, Union
import numpy as np
from datetime import datetime


FloatPairList = Union[Tuple[float], Tuple[float, float]]
XoverResult = Tuple[FloatPairList, FloatPairList, FloatPairList]

def xover_ssh(cds1: np.ndarray, 
              cds2: np.ndarray, 
              pssh1: np.ndarray, 
              pssh2: np.ndarray, 
              pday1: np.ndarray, 
              pday2: np.ndarray, 
              kmcutoff=30.0) -> XoverResult:
    """
	XOVER finds a crossover point between two passes (half orbits) of
	one or two satellites and returns the coordinates of the crossover
	point if any exists.  Lat & Lon are assumed to be in Degrees in 
	addition, it also computes the ssh value and time value at the 
	crossover point in both passes
	
	
	!!!NOTE!!!!: ALL coordinates must be SORTED IN TIME before being
	passed to this function!!!
	
	
	Usage:  xcds, xssh, xday = xover_ssh(cds1,cds2,ssh1,ssh2,day1,day2,kmcutoff)
	
		   XOVER takes coordinates (longitude,latitude) for two 
		   passes, either from the same or different satellites, and
		   finds a crossover point between them, if any exists. SSH1
		   and SSH2 correspond to ssh values at CDS1 and CDS2. DAY1 
		   and DAY2 are corresponding times.
		   KMCUTOFF is a distance from the crossover, within which
		   BOTH passes must have data in order for a crossover to be
		   returned.
	Args:
		   CDS1 - N x 2 array containing [longitude, latitude] of
				  pass from satellite 1
		   CDS2 - N x 2 array containing [longitude, latitude] of
				  pass from satellite 2
		   SSH1 - N x 1 array of SSH from pass1
		   SSH2 - N x 1 array of SSH from pass2
		   DAY1 - N x 1 array of times from pass1
		   DAY2 - N x 1 array of times from pass2
		   KMCUTOFF - both passes must have data within this 
				  distance (in km) from the crossover point or else
				  no cross over is returned. Default is 30 km
	  Returns:
		   XCDS - 1 x 2 array containing [longitude, latitude] of
				  the crossover point.  Set to empty if non exists
		   XSSH - a pair of ssh values, XSSH[0] is ssh from pass1 at 
				  the crossover points.  SXSH[1] is ssh from pass2.
		   XDAY - a pair of time values, XDAY[0] is time from 
				  pass1 at the crossover point.  XDAY1[1] is from pass2.
		"""
	
	# Validate dimensions of input
    if np.size(cds1,axis=1) != 2 or np.size(cds2,axis=1) != 2:
        raise ValueError("Input arrays cds1 and cds2 should be N x 2 arrays of [lon, lat]")
    if (np.size(cds1,axis=0) != np.size(pssh1,axis=0)) | (np.size(cds1,axis=0) != np.size(pday1,axis=0)) :
        raise ValueError("Input arrays cds1 must match size of ssh1 and day1")
    if (np.size(cds2,axis=0) != np.size(pssh2,axis=0)) | (np.size(cds2,axis=0) != np.size(pday2,axis=0)) :
        raise ValueError("Input arrays cds2 must match size of ssh2 and day2")
    if type(pday1[0]) is datetime or type(pday2[0]) is datetime:
        raise ValueError("Day1 & Day2 variables must be floats")

    # sort in time just in case
    ii=np.argsort(pday1)
    cds1=cds1[ii,:];pday1=pday1[ii];pssh1=pssh1[ii]
    ii=np.argsort(pday2)
    cds2=cds2[ii,:];pday2=pday2[ii];pssh2=pssh2[ii]

    # initialize as empty arrays
    xcds = []
    xssh = []
    xday = []
    
    # need to find out if passes are prograde/retrograde & wrapped or not
    dlon1=cds1[-1,0]-cds1[0,0]
    dlon2=cds2[-1,0]-cds2[0,0]
    
    # REMEMBER:  This ASSUMES we DON'T have any valid passes with lon range >180
    if dlon1>180:  # this means the pass is wrapped & retrograde
        ispgrade1=False
        iswrap1=True
    if dlon1<-180:   # wrapped and prograde
        ispgrade1=True
        iswrap1=True
    if (dlon1>0) & (dlon1<180):   # not wrapped & prograde
        ispgrade1=True
        iswrap1=False
    if (dlon1<0) & (dlon1>-180):   # wrapped & not prograde
        ispgrade1=False
        iswrap1=False
    if dlon2>180:    # this means the pass is wrapped & retrograde
        ispgrade2=False
        iswrap2=True
    if dlon2<-180: # wrapped and prograde
        ispgrade2=True
        iswrap2=True
    if (dlon2>0) & (dlon2<180): # not wrapped & prograde
        ispgrade2=True
        iswrap2=False
    if (dlon2<0) & (dlon2>-180): # wrapped & not prograde
        ispgrade2=False
        iswrap2=False

    # start eliminating non-overlaping cases
    if (dlon1==0) | (dlon2==0):
        return [], [], [] # no passes with same starting and endpoint
    
    # start by just keeping all values in the original cds1 & cds2 arrays
    keepii1=np.arange(0,np.size(cds1,axis=0))
    keepii2=np.arange(0,np.size(cds2,axis=0))
        
    # create some bookkeeping values
    if ispgrade1:
        l1min=cds1[0,0]
        l1max=cds1[-1,0]
    if ispgrade2:
        l2min=cds2[0,0]
        l2max=cds2[-1,0]
    if not ispgrade1:
        l1min=cds1[-1,0]
        l1max=cds1[0,0]
    if not ispgrade2:
        l2min=cds2[-1,0]
        l2max=cds2[0,0]
        
    # Case of Neither wrapped
    if (not iswrap1) & (not iswrap2):
        
        # no overlap, return
        if (l1max<l2min) or (l2max<l1min):
            return [], [], []
        
        # limit indicies to overlapping longitudes
        lonbox=[max([l1min,l2min]),min([l1max,l2max])]
        
        jj=np.where((cds1[:,0]>=lonbox[0])&(cds1[:,0]<=lonbox[1]))
        keepii1=keepii1[jj]
        jj=np.where((cds2[:,0]>=lonbox[0])&(cds2[:,0]<=lonbox[1]))
        keepii2=keepii2[jj]
        
        # return if keepii1/2 empty
        if (np.size(keepii1)<2) | (np.size(keepii2)<2):
            return [], [], []
        
    # case of pass 1 wrapped, pass 2 not wrapped
    if (iswrap1) and not (iswrap2):
        
        # test for no overlap
        if (l2min > l1max) & (l2max < l1min):
            return [], [], []
        
        # limit by longitude
        if l2min <= l1max:
            lonbox=[l2min,l1max]
        if l2max >= l1min:
            lonbox=[l1min,l2max]

        jj=np.where((cds1[:,0]>=lonbox[0])&(cds1[:,0]<=lonbox[1]))
        keepii1=keepii1[jj]
        jj=np.where((cds2[:,0]>=lonbox[0])&(cds2[:,0]<=lonbox[1]))
        keepii2=keepii2[jj]

        # return if keepii1/2 empty
        if (np.size(keepii1)<2) | (np.size(keepii2)<2):
            return [], [], []
                
    # case of pass2 wrapped and pass 1 not wrapped
    if not (iswrap1) and iswrap2:
        
        # test for no overlap
        if (l2min > l1max) & (l2max < l1min):
            return [], [], []
        
        # limit by longitude
        if l1min <= l2max:
            lonbox=[l1min,l2max]
        if l1max >= l2min:
            lonbox=[l2min,l1max]

        jj=np.where((cds1[:,0]>=lonbox[0])&(cds1[:,0]<=lonbox[1]))
        keepii1=keepii1[jj]
        jj=np.where((cds2[:,0]>=lonbox[0])&(cds2[:,0]<=lonbox[1]))
        keepii2=keepii2[jj]

        # return if keepii1/2 empty
        if (np.size(keepii1)<2) | (np.size(keepii2)<2):
            return [], [], []
                
    # case of both passes wrapped
    if iswrap1 and iswrap2:
        
        # limit by longitude
        lonbox=[min([l1max,l2max]),max([l1min,l2min])]
        
        # no need to test overlap, since "wrap lon" overlaps in this case
        jj=np.where((cds1[:,0]>=lonbox[1])|(cds1[:,0]<=lonbox[0]))
        keepii1=keepii1[jj]
        jj=np.where((cds2[:,0]>=lonbox[1])|(cds2[:,0]<=lonbox[0]))
        keepii2=keepii2[jj]

        # return if keepii1/2 empty
        if (np.size(keepii1)<2) | (np.size(keepii2)<2):
            return [], [], []
        
    # now limit by latitude (with a bit of margin)

    latbox=np.sort([cds1[keepii1[0],1], cds1[keepii1[-1],1], \
                    cds2[keepii2[0],1], cds2[keepii2[-1],1]],axis=0)

    # check for no overlap first
    jj=np.where((cds1[keepii1,1]>=latbox[1]-.1) & \
                (cds1[keepii1,1]<=latbox[2]+.1))
    keepii1=keepii1[jj]
    
    if np.size(jj)<2:
        return [], [], []

    jj=np.where((cds2[keepii2,1]>=latbox[1]-.1) & \
                (cds2[keepii2,1]<=latbox[2]+.1))
    
    keepii2=keepii2[jj]
    
    # one last check for non overlap
    
    if np.size(jj)<2:
        return [], [], []

    
    # make a reduced version of cds1 & cds2 to speed things up
    scds1=cds1[keepii1,:]
    scds2=cds2[keepii2,:]
    
    # if passes are wrapped, make extra values to handle interpolation
    # across wrap, and add a nan value in the middle to avoid interp
    # across invalid longitudes
    x1start=[]
    y1start=[]
    x1end=[]
    y1end=[]
    x1mid=[]
    x2start=[]
    y2start=[]
    x2end=[]
    y2end=[]
    x2mid=[]
    
    #LATER, CONSIDER REPLACING THE CODE BELOW WITH THIS VERSION THAT DOES NOT DUPLICATE CODE:
    # def process_wraps(scds, iswrap, ispgrade):
    #     '''
    #     Process the wrapped passes
    #     '''
    #     if iswrap:
    #         jumpii = np.where(abs(np.diff(scds[:, 0])) > 180)
    #         if ispgrade:
    #             xend = scds[jumpii[0] + 1, 0] + 360
    #             xstart = scds[jumpii, 0] - 360
    #         else:
    #             xend = scds[jumpii[0] + 1, 0] - 360
    #             xstart = scds[jumpii, 0] + 360
    #         xmid = xend / 2 + xstart / 2
    #         yend = scds[jumpii[0] + 1, 1]
    #         ystart = scds[jumpii, 1]
    #         return xstart, xend, xmid, ystart, yend
    #     else:
    #         return None, None, None, None, None

    # # Call the function for the first set of conditions
    # x1start, x1end, x1mid, y1start, y1end = process_wraps(scds1, iswrap1, ispgrade1)

    # # Call the function for the second set of conditions
    # x2start, x2end, x2mid, y2start, y2end = process_wraps(scds2, iswrap2, ispgrade2)

    if iswrap1:
        jumpii1=np.where(abs(np.diff(scds1[:,0]))>180)
        if ispgrade1:
            x1end=scds1[jumpii1[0]+1,0]+360
            x1start=scds1[jumpii1,0]-360
        else:
            x1end=scds1[jumpii1[0]+1,0]-360
            x1start=scds1[jumpii1,0]+360
        x1mid=x1end/2+x1start/2
        y1end=scds1[jumpii1[0]+1,1]
        y1start=scds1[jumpii1,1]

    if iswrap2:
        jumpii2=np.where(abs(np.diff(scds2[:,0]))>180)
        if ispgrade2:
            x2end=scds2[jumpii2[0]+1,0]+360
            x2start=scds2[jumpii2,0]-360
        else:
            x2end=scds2[jumpii2[0]+1,0]-360
            x2start=scds2[jumpii2,0]+360
        x2mid=x2end/2+x2start/2
        y2end=scds2[jumpii2[0]+1,1]
        y2start=scds2[jumpii2,1]

    # append our special points to coords for interpolation
    x1interp=np.append(scds1[:,0],x1end)
    x1interp=np.append(x1interp,x1start)
    x1interp=np.append(x1interp,x1mid)
    y1interp=np.append(scds1[:,1],y1end)
    y1interp=np.append(y1interp,y1start)
    y1interp=np.append(y1interp,np.nan)

    # interpolate pass1 onto pass2
    ii=np.argsort(x1interp)
    fillat1=np.interp(scds2[:,0],x1interp[ii],y1interp[ii],\
                      left=np.nan,right=np.nan)
        
    # append our special points to coords for interpolation
    x2interp=np.append(scds2[:,0],x2end)
    x2interp=np.append(x2interp,x2start)
    x2interp=np.append(x2interp,x2mid)
    y2interp=np.append(scds2[:,1],y2end)
    y2interp=np.append(y2interp,y2start)
    y2interp=np.append(y2interp,np.nan)

    # interpolate pass1 onto pass2
    ii=np.argsort(x2interp)
    fillat2=np.interp(scds1[:,0],x2interp[ii],y2interp[ii],\
                      left=np.nan,right=np.nan)
    
    # subtract interpolated latitudes
    dellat1=scds2[:,1]-fillat1
    dellat2=scds1[:,1]-fillat2
    
    # find indices where sign changes
    xind2=np.where(abs(np.diff(np.sign(dellat1)))==2)
    xind2=np.array(xind2) # make sure this is ndarray for testing

    if np.size(xind2)==0: 
        return [], [], []
    xind2=np.append(xind2[0],xind2[0]+1)
    xind1=np.where(abs(np.diff(np.sign(dellat2)))==2)
    xind1=np.array(xind1) # make sure this is ndarray for testing

    if np.size(xind1)==0: 
        return [], [], []
    xind1=np.append(xind1[0],xind1[0]+1)
    
    # now that we have the points, use formula for straight line to
    # calculate the excact position of the crossover
    wrappoint1=False
    wrappoint2=False

    if len(xind1)==1: # exact match
        xcds=scds1[xind1,:]
        x1=scds1[xind1,0]
        y1=scds1[xind1,1]
        x2=x1
        y2=y1
        sp1=pssh1[keepii1[xind1]]
        sd1=pday1[keepii1[xind1]]
    if len(xind2)==1: # exact match
        xcds=scds2[xind2,:]
        x3=scds2[xind2,0]
        y3=scds2[xind2,1]
        x4=x3
        y4=y3
        sp2=pssh2[keepii2[xind2]]
        sd2=pday2[keepii2[xind2]]
    if (len(xind1)==2)&(len(xind2)==2):
        x1=scds1[xind1[0],0]
        y1=scds1[xind1[0],1]
        x2=scds1[xind1[1],0]
        y2=scds1[xind1[1],1]
        x3=scds2[xind2[0],0]
        y3=scds2[xind2[0],1]
        x4=scds2[xind2[1],0]
        y4=scds2[xind2[1],1]
        ps1=pssh1[keepii1[xind1]]
        ps2=pssh2[keepii2[xind2]]
        pd1=pday1[keepii1[xind1]]
        pd2=pday2[keepii2[xind2]]
        if ispgrade1:
            if x2<x1:
                x2=x2+360
                wrappoint1=True
        else:
            if x1<x2:
                x2=x2+360
                wrappoint1=True
        if ispgrade2:
            if x4<x3:
                x4=x4+360
                wrappoint2=True
        else:
            if x3<x4:
                x4=x4+360
                wrappoint1=True
        # compute slopes of latitude lines
        ma = (y2-y1)/(x2-x1)
        mb = (y4-y3)/(x4-x3)
        # compute intersection of lats
        x=(y3-y1-mb*x3+ma*x1)/(ma-mb)
        y=ma*(x-x1)+y1
        xcds=[x,y]
        # compute ssh & day values for pass 1
        sp1=np.interp(x,[x1,x2],ps1,left=np.nan,right=np.nan)
        sd1=np.interp(x,[x1,x2],pd1,left=np.nan,right=np.nan)
        # compute ssh & day values for pass 2    
        sp2=np.interp(x,[x3,x4],ps2,left=np.nan,right=np.nan)
        sd2=np.interp(x,[x3,x4],pd2,left=np.nan,right=np.nan)

    # save day and ssh from both passes into return variables
    xday=[sd1,sd2]
    xssh=[sp1,sp2]
    
    # run a test to make sure that the crossover point isn't too far away
    # from the nearest real datapoints, based on kmcutoff threshold
    dst1=np.sqrt(((y1-y)*111)**2+((x1-x)*111*np.cos((y1/2+y/2)*np.pi/180))**2)
    dst2=np.sqrt(((y2-y)*111)**2+((x2-x)*111*np.cos((y2/2+y/2)*np.pi/180))**2)
    dst3=np.sqrt(((y3-y)*111)**2+((x3-x)*111*np.cos((y3/2+y/2)*np.pi/180))**2)
    dst4=np.sqrt(((y4-y)*111)**2+((x4-x)*111*np.cos((y4/2+y/2)*np.pi/180))**2)
   
    if any(dst > kmcutoff for dst in (dst1, dst2, dst3, dst4)):
        return [], [], []
    
    # just in case xcds needs to be unwrapped
    if wrappoint1 | wrappoint2:
        if (x1>360) | (x2>360):
            if xcds[0]>360:
                xcds[0]=xcds[0]-360
        else:
            if xcds[0]>180:
                xcds[0]=xcds[0]-360
                    
    return xcds, xssh, xday

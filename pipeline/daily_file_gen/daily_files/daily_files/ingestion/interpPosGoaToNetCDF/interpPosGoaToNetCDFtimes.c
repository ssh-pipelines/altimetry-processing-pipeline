/*******************************************************************************************
* FUNCTION: INTERPPOSGOATONETCDFTIMES
* Purpose:
* To interpolate a pos_goa file to epochs specified in an input netCDF file and return the 
* interpolated orbit in geodetic coordinates, latitute/longitude/altitude, computed
* using the flattening coefficient and Earth's semi-major axis specified in the global 
* attributes of the netCDF file input
* Leap second files can be found and are regularly updated at
* https://www.ietf.org/timezones/data/leap-seconds.list
* If specified, a netCDF file is written in output.
* Adapted from Shailen Desai's diff2posgoas.c program
*******************************************************************************************/
#include "interpPosGoaToNetCDFtimes.h"

int main(int argc, char *argv[])
{
   static double torb[MAXORB], Xorb[MAXORB], Yorb[MAXORB], Zorb[MAXORB];
   int32_t i, ipos, incdf, norb;

   if (argc < 2){
       usage();
       return 0;
   }

   ipos  = 0;
   incdf = 0;
   for (i = 1; i < argc; ++i) {
     if ((strcmp(argv[i],"-h") == 0) || (strcmp(argv[i],"-H")==0) || (strcmp(argv[i],"-help")==0)) {
       usage();
       return 0;
     }  
     if (strcmp(argv[i],"-posgoa") == 0) {
       ++i;
       if (i >= argc) {
         break;
       }
       ipos = i;
     }
     if (strcmp(argv[i],"-netcdf") == 0) {
       ++i;
       if (i >= argc) {
         break;
       }
       incdf = i;
     }
   }
   
   if (ipos == 0) {
     fprintf(stdout,"-posgoa option is required.\n");
     exit(1);
   }
   if (incdf == 0) {
     fprintf(stdout,"-netcdf option is required.\n");
     exit(1);
   }

// Read the pos_goa file with the orbit
// Note that pos_goa files provide ECEF X, Y, Z coordinates of spacecraft at GPS time (seconds since Jan 1, 2000 12:00:00)
   if (readposgoafile(torb, Xorb, Yorb, Zorb, &norb, argv[ipos], MAXORB) == 1) {
     fprintf(stderr,"ERROR: Could not read file %s\n", argv[ipos]);
     exit(1);
   }

// Interpolate the pos_goa orbit file to the times on the netCDF product
   if (interpPosGoaToncfile(argv[incdf], torb, Xorb, Yorb, Zorb, norb) == 1) {
     fprintf(stderr, "ERROR: Could not interpolate orbit file to times on netCDF file %s\n", argv[incdf]);
     exit(1);
   } 

   return 0;
}

/********************************************************************************
 * * USAGE
 ******************************************************************************/
void usage(void)
{
   printf("\nUsage: this routine will interpolate a JPL 335A pos_goa orbit file to epochs specified in an input netCDF file.\n" 
                 "       The routine returns the geodetic coordinates of the interpolated orbit.\n"
                 "       The latitude, longitude and altitude are computed using the Earth's flattening coefficient and semi-major axis\n" 
                 "       specified in the global attributes of the netCDF file input.\n"
                 "       The epochs in pos_goa orbit files are in GPS time since Jan 1, 2000 12:00:00.\n"
                 "       The TAI epochs from the altimetry product files is used which are TAI time since Jan 1, 2000 00:00:00.\n"
                 "       TAI time - GPS time = 19.0 seconds"
                 "Options:\n"
                 "   -posgoa: name of pos_goa orbit file in uncompressed format (required)\n"
                 "   -netcdf:  name of netCDF altimetry file (required)\n"
                 "Example:\n"
                 "   ./interpPosGoaToNetCDFtimes.e -posgoa 2016-05-16.JAS3.leo_ef.pos \\ \n"
                 "\n");
    }     

/********************************************************************************
 * * FUNCTION: READNCFILE
 * * Purpose:
 * * To read a netCDF file containing the epochs at which to interpolate as
 * * well as the Earth's flattening and radius to use for lat/lon/alt conversion
 * * 
 * * Input:
 * *   ncfile[]     - Name of netCDF file
 * * Output:
 * *   tncdf[]      - Array of time tags (UTC time) J2000 sec
 * *   slancdf[]    - Array of sla (m)
 * *   altncdf[]    - Array of alt (m)
 * *   nncdf        - Number of epochs in file
 * **************************************************************************/
int interpPosGoaToncfile(char ncfile[], double torb[], double Xorb[], double Yorb[],
                         double Zorb[], int32_t norb)
{
   double *ttai;
   int *lat, *lon, *alt, *ssha;
   int ncid; // netCDF file ID
   int data01_ncid; // 1 Hz data group ID
   int ku_ncid; // 1 Hz data group ID
   int time_dimid; // netCDF time dimension ID
   double ellipsoid_axis; // Reference ellipsoid axis
   double ellipsoid_flattening; // Reference ellipsoid flattening
   size_t time_len; // Time dimension length
   int taitime_id, lat_id, lon_id, alt_id, ssha_id;
   int alt_fillvalue;
   int ssha_fillvalue;
   double lat_scale_factor, lon_scale_factor, alt_scale_factor, alt_add_offset, ssha_scale_factor;
   double xyz[3], llh[3], xyzdot[3], tgps, latd, lond, altd, diffalt, sshad, newssha;
   double rmsalt, nalt;
   int32_t i;
   static size_t start[1] = {0};

   /* Open the netCDF file. */
   NCERR( nc_open(ncfile, NC_NOWRITE, &ncid), ncfile);

   /* Get the 1 Hz data group id */
   NCERR( nc_inq_grp_ncid(ncid, "data_01", &data01_ncid), ncfile);

   /* Get the 1 Hz Ku-band data group id */
   NCERR( nc_inq_grp_ncid(data01_ncid, "ku", &ku_ncid), ncfile);

   /* Get the 1 Hz time dimension of the file */
   NCERR( nc_inq_dimid(data01_ncid, "time", &time_dimid), ncfile);
   NCERR( nc_inq_dimlen(data01_ncid, time_dimid, &time_len), ncfile);

   /* Create memory for variables */
   ttai  = malloc(time_len * sizeof(double));
   lat   = malloc(time_len * sizeof(int));
   lon   = malloc(time_len * sizeof(int));
   alt   = malloc(time_len * sizeof(int));
   ssha  = malloc(time_len * sizeof(int));

   /* Get the reference ellipsoid parameters from the netCDF file */
   NCERR( nc_get_att_double(ncid, NC_GLOBAL, "ellipsoid_semi_major_axis", &ellipsoid_axis), ncfile);
   NCERR( nc_get_att_double(ncid, NC_GLOBAL, "ellipsoid_flattening", &ellipsoid_flattening), ncfile);

   /* Get netCDF file variables */

   /* Read TAI time */
   NCERR( nc_inq_varid(data01_ncid, "time_tai", &taitime_id), ncfile);
   NCERR( nc_get_vara_double(data01_ncid, taitime_id, start, &time_len, ttai), ncfile);

   /* Read latitude */
   NCERR( nc_inq_varid(data01_ncid, "latitude", &lat_id), ncfile);
   NCERR( nc_get_vara_int(data01_ncid, lat_id, start, &time_len, lat), ncfile);
   NCERR( nc_get_att_double(data01_ncid, lat_id, "scale_factor", &lat_scale_factor), ncfile);

   /* Read longitude */
   NCERR( nc_inq_varid(data01_ncid, "longitude", &lon_id), ncfile);
   NCERR( nc_get_vara_int(data01_ncid, lon_id, start, &time_len, lon), ncfile);
   NCERR( nc_get_att_double(data01_ncid, lon_id, "scale_factor", &lon_scale_factor), ncfile);

   /* Read altitude */
   NCERR( nc_inq_varid(data01_ncid, "altitude", &alt_id), ncfile);
   NCERR( nc_get_vara_int(data01_ncid, alt_id, start, &time_len, alt), ncfile);
   NCERR( nc_get_att_double(data01_ncid, alt_id, "scale_factor", &alt_scale_factor), ncfile);
   NCERR( nc_get_att_double(data01_ncid, alt_id, "add_offset", &alt_add_offset), ncfile);
   NCERR( nc_get_att_int(data01_ncid, alt_id, "_FillValue", &alt_fillvalue), ncfile);

   /* Read ssha */
   NCERR( nc_inq_varid(ku_ncid, "ssha", &ssha_id), ncfile);
   NCERR( nc_get_vara_int(ku_ncid, ssha_id, start, &time_len, ssha), ncfile);
   NCERR( nc_get_att_double(ku_ncid, ssha_id, "scale_factor", &ssha_scale_factor), ncfile);
   NCERR( nc_get_att_int(ku_ncid, ssha_id, "_FillValue", &ssha_fillvalue), ncfile);

   /* Close the netCDF file */
   NCERR( nc_close(ncid), ncfile);

   /* Loop through each 1 Hz data record on netCDF file */
   rmsalt = 0.0;
   nalt   = 0.0;
   for (i = 0; i < time_len; ++i) {
      /* Convert TAI time on netCDF (since Jan 1, 2000 00:00:00) to GPS time on orbit file (since Jan 1, 2000, 12:00:00) */
      tgps = ttai[i] - TAIMGPS - 43200.0;

      /* Check that netCDF file time is within range of time on orbit file */
      if (tgps < torb[0]) {
         fprintf(stdout,"WARNING: netCDF file time is before first time on orbit file: ttai = %f\n", ttai[i]);
         continue;
      }
      if (tgps > torb[norb-1]) {
         fprintf(stdout,"WARNING: netCDF file time is after last time on orbit file: ttai = %f\n", ttai[i]);
         continue;
      }

      /* Interpolate orbit to time on netCDF file */
      /* llh[3] = lat, lon, altitude */
      if (interporb(xyz, llh, xyzdot, tgps, ellipsoid_axis, ellipsoid_flattening, torb, Xorb, Yorb, Zorb, norb) != 0) {
         fprintf(stdout, "WARNING: Error interpolating orbit at tai = %f\n", ttai[i]);
         continue;
      }

      /* Convert longitude to 0 to 360 range */
      if (llh[1] < 0.0) {
         llh[1] += 360.0;
      }

      /* Check the latitude from product to interpolated orbit as sanity check */
      latd = ((double) lat[i])*lat_scale_factor;
      lond = ((double) lon[i])*lon_scale_factor;
      if (fabs(latd - llh[0]) > 5.0e-6) {
         fprintf(stdout,"WARNING: Latitude from interpolated orbit and on product different by more than 5e-6 deg at ttai = %f\n", ttai[i]);
      }
      if (fabs(lond - llh[1]) > 5.0e-6) {
         fprintf(stdout,"WARNING: Longitude from interpolated orbit and on product different by more than 5e-6 deg at ttai = %f\n", ttai[i]);
      }

      altd = ((double) alt[i])*alt_scale_factor  + alt_add_offset;
      /* Check on altitude */
      diffalt = llh[2] - altd;
      if (fabs(diffalt) > 1.0e3) {
         fprintf(stdout,"WARNING: Altitude from interpolated orbit and on product different by more than 1 m at ttai = %f\n", ttai[i]);
      }
      rmsalt += diffalt*diffalt;
      nalt   += 1.0;

      /* Compute SSHA from interpolated orbit instead of netCDF file orbit */
      sshad = DEFVALUE;
      if (ssha[i] != ssha_fillvalue) {
          sshad   = ((double) ssha[i])*ssha_scale_factor;
          newssha = sshad + diffalt;
      }
      else {
          newssha = DEFVALUE;
      }

      /* Printing updated ssha */
      /* Record number, TAI time from netCDF file, latitude from netCDF file, longitude from netCDFfile, altitude from netCDF file,
       *                                           latitude from orbit file, longitude from orbit file, altitude from orbit file
       *                                           netCDF - orbit file altitude, SSHA form netCDF file, SSHA from orbit file */
      fprintf(stdout,"%6d  %18.6f  %12.6f  %12.6f  %15.3f  %12.6f  %12.6f  %15.3f  %12.6f  %12.6f  %12.6f\n", i, ttai[i], latd, lond, altd, llh[0], llh[1], llh[2], diffalt, sshad, newssha);
   }
   if (nalt > 0.0) {
      rmsalt = sqrt(rmsalt/nalt);
   }

   /* Free memory */
   free(ttai);
   free(lat);
   free(lon);
   free(alt);
   free(ssha);
 
   fprintf(stdout, "*** SUCCESS interpolating netCDF file. RMS of orbit difference = %f mm!\n", rmsalt*1.0e3);
   return 0;
}
/**************************************************************************
* To read a pos-goa file
* 
* Input:
*   posgoafile[] - Name of JPL pos_goa file with orbit
*   ndim         - Array size of torb, Xorb, Yorb, Zorb
* Output:
*   torb[]       - Array of time tags (GPS time, seconds since Jan 1, 2000 12:00:00)
*   Xorb[]       - Array of X coordinates (m)
*   Yorb[]       - Array of X coordinates (m)
*   Zorb[]       - Array of X coordinates (m)
*   norb         - Number of points in orbit array
**************************************************************************/
int readposgoafile(double torb[], double Xorb[], double Yorb[], double Zorb[],
                   int32_t *norb, char posgoafile[], int32_t ndim)
{
    double t, x, y, z;
    int32_t   i, it;
    char   line[500], frame[10], satname[20];
    FILE   *ifin;

    if ((ifin = fopen(posgoafile,"r")) == NULL) {
      fprintf(stderr,"ERROR: Could not open file %s\n", posgoafile);
      return 1;
    }
    i = 0;
    while (fgets(line,MAXLINE,ifin) != NULL) {
      sscanf(line,"%s %s %d %lf %lf %lf %lf", frame, satname, &it, &t, &x, &y, &z);
      if (i >= ndim) {
        fprintf(stderr,"ERROR: Reset orbit dimension to > %d\n", ndim);
	fclose(ifin);
	return 1;
      }
      torb[i] = ((double) it) + t;
      // Convert from km to m
      Xorb[i] = 1.0e3*x;
      Yorb[i] = 1.0e3*y;
      Zorb[i] = 1.0e3*z;
      i       = i + 1;
    }
    fclose(ifin);
    *norb = i;

    fprintf(stdout,"*** SUCCESS reading pos-goa file. %d records\n", *norb);
    return 0;
}
/**************************************************************************
* FUNCTION: INTERPORB
* Purpose:
* To interpolate an orbit given an array of x,y,z, coordinates
*
* Input:
*   t            - Time at which orbit required (GPS time, seconds since Jan 1, 2000 12:00:00)
*   ae           - Equatorial radius of reference ellipsoid (unit)
*   flat         - Flattening of reference rellipsoid
*   torb[]       - Array of time tags (GPS time, seconds since Jan 1, 2000 12:00:00)
*   xorb[]       - Array of X coordinates (unit)
*   yorb[]       - Array of Y coordinates (unit)
*   zorb[]       - Array of Z coordinates (unit)
*   norb         - Number of point in orbit array
*
* Output:
*   xyz[3]       - x,y,z coordinates (unit)
*   llh[3]       - Latitude (deg), Longitude (deg), height (unit)
*   xyzdot[3]    - x,y,z velocities (unit/sec)
* FUNCTION returns 1 is error interpolating orbit
**************************************************************************/
int interporb(double xyz[3], double llh[3], double xyzdot[3],
	      double t, double ae, double flat, double torb[],
	      double xorb[], double yorb[], double zorb[], int32_t norb)
{
   int32_t   i;

// Interpolate x, y, z coordinates
   for (i = 0; i < 3; ++i) {
     xyz[i] = 0.0;
     llh[i] = 0.0;
   }
   if (intlagrange(t, &xyz[0], norb, torb, xorb, 7, 1, &xyzdot[0]) != 0) {
     fprintf(stderr,"interporb: Error interpolating orbit: t = %f\n", t);
     return 1;
   }
   if (intlagrange(t, &xyz[1], norb, torb, yorb, 7, 1, &xyzdot[1]) != 0) {
     fprintf(stderr,"interporb: Error interpolating orbit: t = %f\n", t);
     return 1;
   }
   if (intlagrange(t, &xyz[2], norb, torb, zorb, 7, 1, &xyzdot[2]) != 0) {
     fprintf(stderr,"interporb: Error interpolating orbit: t = %f\n", t);
     return 1;
   }
   if (xyz2gd(&llh[0], &llh[1], &llh[2], xyz[0], xyz[1], xyz[2], ae, flat) == 1) {
     fprintf(stderr, "interporb: Error converting to llh\n");
     return 1;
   }

   return 0;
}

/**********************************************************************
* FUNCTION: XYZ2GD
* Purpose:
* To convert geocentric x,y,z coordinates into geodetic latitude,
* longitude and height
* Input:
*   x       - Geocentric X-coordinate (unit)
*   y       - Geocentric Y-coordinate (unit)
*   z       - Geocentric Z-coordinate (unit)
*   ae      - Radius of the Earth (unit)
*   f       - Flattening of the Earth  (1.0/298.257 for Earth)
*             if (flattening <= 0.0, then assumed that f = 0.0
*             and sphere is assumed (Geodetic quantities are then
*             equivalent to geocentric quantities))
* Output:
*   glat    - Geodetic latitude (deg) (<= +- 90.0 deg)
*   glon    - Geodetic longitude (deg) (>= 0 and < 360.0)
*   ght     - Geodetic height (unit)
**********************************************************************/
int xyz2gd(double *glat, double *glon, double *ght, double x,
	   double y, double z, double ae, double f)
{
   double r, p, lon, lat, h, oof, esq, x0, y0, hm, N, sn;
   double Nw2g, uw2g, vw2g, uw2h, vw2h, det, u, v, epsuv;
   int    nit;

   if (f < 0.0) {
     fprintf(stderr,"ERROR: xyz2gd - Flattening must be >= 0.0");
     return 1;
   }
   r     = sqrt(x*x + y*y + z*z);
   p     = sqrt(x*x + y*y);
   epsuv = ae*GEODET_EPSUV;

// First deal with case at poles
   if (p == 0.0) {
     if (z == 0.0) {
       fprintf(stderr,"ERROR: xyz2gd - Error all three components are equal to zero\n");
       return 1;
     }
     else {
       if (z > 0.0) {
	 lon = 0.0;
	 lat = PIO2;
	 h   = fabs(z) - ae*(1.0 - f);
       }
       else {
	 lon = 0.0;
	 lat = -PIO2;
	 h   = fabs(z) - ae*(1.0 - f);
       }
     }
   }
   else {
// Compute longitude from 0 to 2pi
     if (x == 0.0) {
       if (y > 0.0) {
	 lon = PIO2;
       }
       else {
	 lon = 3.0*PIO2; 
       }
     }
     else {
       lon = atan2(y, x);
       if (lon < 0.0) {
	 lon += TWOPI;
       }
     }
// Compute geocentric latitude and height 
     if (f <= 0.0) {
       lat = atan(z/p);
       h   = r - ae;
     }
     else {
// Compute first approximation of geodetic latitude and height
       oof = 1.0/f;
       esq = f*(2.0 - f);
       lat = atan((oof*oof*z)/(p*(1.0-oof)*(1.0-oof)));
       N   = ae/(sqrt(1.0 - esq*sin(lat)*sin(lat)));
       x0  = N*cos(lat);
       y0  = N*(1.0 - esq)*sin(lat);
       hm  = sqrt((p-x0)*(p-x0) + (z-y0)*(z-y0));
       sn  = r - sqrt(x0*x0 + y0*y0);
       if (sn < 0.0) {
	 h = -hm;
       }
       else {
	 h = hm;
       }

// Iterate for geodetic latitude and height
       u   = (N + h)*cos(lat) - p;
       v   = (N*(1.0 - esq) + h)*sin(lat) - z;
       nit = 0;
       while ((fabs(u) > epsuv) || (fabs(v) > epsuv)) {
	 if (nit > 100) {
	   fprintf(stderr,"ERROR: xyz2gd - Does not converge on latitude and height\n");
	   exit(1);
	 }

// Partials
	 Nw2g = (N/ae)*(N/ae)*N*esq*sin(lat)*cos(lat);
	 uw2g = Nw2g*cos(lat) - (N + h)*sin(lat);
	 vw2g = Nw2g*sin(lat)*(1.0 - esq) + (N*(1.0 - esq) + h)*cos(lat);
	 uw2h = cos(lat);
	 vw2h = sin(lat);

// Corrections
	 det  = uw2g*vw2h - uw2h*vw2g;
	 lat += (-vw2h*u + uw2h*v)/det;
	 h   += ( vw2g*u - uw2g*v)/det;

// Error
         N   = ae/(sqrt(1.0 - esq*sin(lat)*sin(lat)));
	 u   = (N + h)*cos(lat) - p;
	 v   = (N*(1.0 - esq) + h)*sin(lat) - z;
	 ++nit;
       }
     }
   }

/* Geodetic height
*  if (f > 0.0) {
*    esq = f*(2.0 - f);
*    num = z*(1.0 - f)*r + z*esq*ae;
*    den = p*r;
*    u   = atan2(num, den);
*    num = z*(1.0 - f)  + esq*ae*sin(u)*sin(u)*sin(u);
*    den = (1.0 - f)*(p - esq*ae*cos(u)*cos(u)*cos(u));
*    lat = atan2(num, den);
*    h   = p*cos(phi) + z*sin(phi) - ae*sqrt(1.0 - esq*sin(phi)*sin(phi));
*  }
*/

// Convert to degrees
   *glon = lon/DTR;
   *glat = lat/DTR;
   *ght  = h;

// Checks on bounds
   if ((*glat < -90.0) || (*glat > 90.0)) {
     fprintf(stderr,"ERROR: xyz2gd - Error with bounds of latitude\n");
     return 1;
   }
   if (*glon > 180.0) {
     *glon = *glon - 360.0;
   }
   // if (*glon < 0.0) {
   //   *glon = *glon + 360.0;
   // }

   return 0;
}

/******************************************************************************
*      RTG Source Code,                                                       *
*      Copyright (C) 1996, California Institute of Technology                 *
*      U.S. Government Sponsorship under NASA Contract NAS7-1260              *
*                    (as may be time to time amended)                         *
*                                                                             *
*      RTG is a trademark of the California Institute of Technology.          *
*                                                                             *
*                                                                             *
*      written by Yoaz Bar-Sever, Willy Bertiger, Bruce Haines,               *
*                 Angelyn Moore, Ron Muellerschoen, Tim Munson,               *
*                 Larry Romans, and Sien Wu                                   *
*                                                                             *
*      modified by Gerhard L.H. Kruizinga for stand alone use                 *
*                 7/22/98                                                     *
******************************************************************************/
/* Interpolates an ECI file to retrieve satellite state at the requested time*/
/* Yoaz Bar-Sever. May, 1996 */
#define MAX_DEG 20
/* Performs straightforward Lagrange interpolation,
  to get value y(x) and (if requested) derivative y'(x),
  given tables of x-y points xt[] and yt[].  Tables should
  be equally spaced in x; will still work otherwise, but
  search used is stupidest possible.

    ntab = size of tables
    ndeg = degree of polynomial (uses ndeg+1 points around target x,
           symmetrically distributed if possible)
*/
int intlagrange( double x, double *y, int ntab, double *xt, double *yt, 
                  int ndeg, int compute_deriv, double *yd)
{

  double i_r, yyd;
  int i_shift, i, i1, i2, j, k, n, n2;

  static double w[MAX_DEG], df[MAX_DEG], x0_save, x_save;
  double *xi, *yi;
  static int n_save = -1;
  static int i1_save;

  if (n_save == ndeg + 1 && x == x_save && xt[0] == x0_save
       && compute_deriv == 0) {
    *y = 0.0;
    yi = yt + i1_save;
    for (i = 0; i < n_save; ++i) {
      *y += w[i]*yi[i];
    }
    return (int) 0;
  }

  if (x < xt[0] ) { return (int) -1;}
  if (x > xt[ntab-1]) { return (int) 1; }

  if (ntab <= ndeg) {
    return (int) 2;
  }

  i_r = (ntab - 1) * (x - xt[0])/(xt[ntab-1] - xt[0]);
  i = (int) floor(i_r);
  if (i == ntab - 1) i--;
  i_r = i_r - i;

  if (i == ntab-1) {
    i--;
    i_r++;
  }

  if (x < xt[i] || x > xt[i+1]) {
    i_shift = 0;
    if (x < xt[i]) {
      while (x < xt[i]) { i_shift--; i--; }
    }
    else {
      while (x > xt[i+1]) { i_shift++; i++; }
    }
    i_r = (x - xt[i])/(xt[i+1] - xt[i]);
  }

  n = ndeg + 1;
  if (n % 2) {
    /* n odd */
    n2 = (n-1)/2;
    if (i_r < 0.5) {
      i1 = i - n2;
      i2 = i + n2;
    }
    else {
      i1 = i - n2 + 1;
      i2 = i + n2 + 1;
    }
  }
  else {
    n2 = n/2;
    i1 = i - n2 + 1;
    i2 = i + n2;
  }
  
  if (i1 < 0) {
    i1 = 0;
    i2 = n - 1;
  }
  if (i2 >= ntab) {
    i2 = ntab - 1;
    i1 = ntab - n;
  }

  i1_save = i1;
  xi = xt + i1;
  yi = yt + i1;

  for (i = 0; i < n; ++i) {
    df[i] = 1.0;
    for (j = 0; j < n; ++j) {
      if (j != i) {
        df[i] /= (xi[i] - xi[j]);
      }
    }
  }

  *y = 0.0;
  for (i = 0; i < n; ++i) {
    w[i] = df[i];
    for (j = 0; j < n; ++j) {
      if (j != i) {
        w[i] *= (x - xi[j]);
      }
    }
    *y += w[i]*yi[i];
  }

  if (compute_deriv == 1) {
    *yd = 0.0;
    for (i = i1; i <= i2; i++) {
      for (j = i1; j <= i2; j++) {
        if (j != i) {
          yyd = yt[i]/(xt[i] - xt[j]);
          for (k = i1; k <= i2; k++) {
            if (k != i && k != j) yyd *= (x - xt[k])/(xt[i] - xt[k]);
          }
          *yd += yyd;
        }
      }
    }
  }

  n_save = n;
  x0_save = xt[0];
  x_save = x;

  return (int) 0;
}
/**************************************************************************
* FUNCTION: NCERR
* Purpose: To check for and handle errors with interfacing with NetCDF
* files
*
* Input:
*   status - Status from NetCDF command
*   ncfile - Name of netCDF file
**************************************************************************/
int NCERR(int status, char ncfile[])
{

  if (status != NC_NOERR) {
    fprintf(stderr,"ERROR: %s\n", nc_strerror(status));
    fprintf(stderr,"ERROR: interpPosGoaToNetCDFtimes could not process %s\n", ncfile);
    exit(2);
  }
}

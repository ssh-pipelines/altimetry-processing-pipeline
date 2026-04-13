#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <ctype.h>
#include <netcdf.h>

#define  PI                        (4.0*atan(1.0))
#define  TWOPI                     (2.0*PI)
#define  PIO2                      (PI/2.0)
#define  DTR                       (PI/180.0)
#define  GEODET_EPSUV              1.0e-14

#define  MAXORB                    30000 
#define  MAXLINE                   500

#define  TAIMGPS                   19.0

#define  DEFVALUE                  9999999999.0

void usage(void);
int readposgoafile(double torb[], double Xorb[], double Yorb[], double Zorb[],
                   int32_t *norb, char posgoafile[], int32_t ndim);
int interpPosGoaToncfile(char ncfile[], double torb[], double Xorb[], double Yorb[],
                         double Zorb[], int32_t norb);
int interporb(double xyz[3], double llh[3], double xyzdot[3],
              double t, double ae, double flat, double torb[],
	      double xorb[], double yorb[], double zorb[], int32_t norb);
int xyz2gd(double *glat, double *glon, double *ght, double x,
           double y, double z, double ae, double f);
int intlagrange( double x, double *y, int ntab, double *xt, double *yt,
                  int ndeg, int compute_deriv, double *yd);
int NCERR(int status, char ncfile[]);

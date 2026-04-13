CC        = gcc #gcc-4.9

LIBS      = -lm -lnetcdf -O3

CFLAGS    = -O3 -c

OBJ = interpPosGoaToNetCDFtimes.o
INC = interpPosGoaToNetCDFtimes.h

interpPosGoaToNetCDFtimes: $(OBJ)
	$(CC) $(OBJ) $(LIBS) -o interpPosGoaToNetCDFtimes.e

interpPosGoaToNetCDFtimes.o: interpPosGoaToNetCDFtimes.c $(INC)
	$(CC) $(CFLAGS) interpPosGoaToNetCDFtimes.c

clean :
	/bin/rm -f *.o interpPosGoaToNetCDFtimes.e a.out *~

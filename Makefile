LDFLAGS=libz.a -lc -lm -static

all: comprestimator

comprestimator:	comprestimator.c
	gcc -g -o comprestimator.o -c comprestimator.c
	gcc -g -o comprestimator comprestimator.o libz.a -lc -lm
	rm -f comprestimator.o

clean:
	rm -f comprestimator

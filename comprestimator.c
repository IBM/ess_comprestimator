/* Comprestimator V2.0 -- 20.2.2012
 Authors: Avishay Traeger, Danny Harnik, Dmitry Sotnikov
*/

#define _LARGE_FILES
#include <stdio.h>
#include <fcntl.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <time.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/types.h>
#include "zlib.h"

#if defined(MSDOS) || defined(WIN32)
#include <io.h>
#define SET_BINARY_MODE(file) setmode(fileno(file), O_BINARY)
#else
#define SET_BINARY_MODE(file)
#endif

#define MAX_NUM_SAMPLE		2000	//Max number of non-zero samples to take
#define ZERO_BLOCK_FACTOR	10	    //Ratio of zero blocks to non-zero
#define INBLOCK_SIZE		2048 	//Input block size in bytes (read from disk)
#define ZLIB_BLOCK_SIZE		16384 	//Input block size to zlib in bytes 
#define OUTBLOCK_SIZE		2048	//Output block size in bytes (close gzip)
#define COMP_UNIT_SIZE		134217728	//Input to streamer in bytes (=128MB)
#define BLOCKS_PER_PROC		50	//How many blocks each process should handle (random)
#define MAX_NUM_PROCS		128	//Maximum number of child processes
#define MAX_STRING_LEN		256	//Maximum length of statically allocated strings

#define DEBUG	0
#define debug_print(fmt, ...) \
	do { if (DEBUG) fprintf(stderr, "%d:%d: " fmt, getpid(), __LINE__, __VA_ARGS__); } while (0)

#define min(x, y) ({                            \
        typeof(x) _min1 = (x);                  \
        typeof(y) _min2 = (y);                  \
        (void) (&_min1 == &_min2);              \
        _min1 < _min2 ? _min1 : _min2; })

/* Statistics that each child calculates and the parent aggregates */
struct compression_info {
	int num_zero_blocks;
	int num_non_zero_blocks;
	int total_blocks_read;
	double compression_ratio;
    double c_squared;
};

/* Array of stats in shared memory where each child process stores its stats,
 * and the parent aggregates into the last index */
static struct compression_info *comp_info_array = NULL;

/* Size of the comp_info_array */
static size_t shared_mem_size;

/* Array of child PIDs. A child whose PID appears in index i of this array will
 * puts its stats in index i of comp_info_array */
static pid_t pid_array[MAX_NUM_PROCS];

/* Number of child processes to run (command line parameter) */
static int num_procs = 1;

/* Device to run on */
static char *dev_name = NULL;
static off_t dev_size;

/* Time we began to run the program */
static time_t start_time;

/* Output files */
static FILE *log_file = NULL;
static FILE *csv_file = NULL;
static FILE *res_file = NULL;

/* Get the size of the device in bytes */
static off_t get_dev_size()
{
	int fd;
	off_t size;

	fd = open(dev_name, O_RDONLY);
	if (fd == -1) {
		perror("open");
		return -1;
	}
	size = lseek(fd, -1, SEEK_END);
	if (size == -1) {
		perror("lseek");
		return -1;
	}
	close(fd);
	return size;
}

/* Is the block all zeroes? */
static int is_zero_block(char *buf) {
	/* I assume memcmp is optimized, so use it by checking if first byte is
	 * zero, and every byte is the same as the previous */
	return ((buf[0] == 0) && (!memcmp(buf, buf + 1, INBLOCK_SIZE - 1)));
}

void usage(char *prog)
{
	fprintf(stderr, "usage: %s -d <dev_name> [-p <num_procs> -l <log_file> -c <csv_file> -r <res_file> -s <seed> -e -h]\n", prog);
	fprintf(stderr, "       -d: path to device to process\n");
	fprintf(stderr, "       -p: number of processes (default 1)\n");
	fprintf(stderr, "       -l: log file for intermediate results, errors, debug messages(text format)\n");
	fprintf(stderr, "       -c: log file for intermediate results (csv format)\n");
	fprintf(stderr, "       -r: file for final results (csv format)\n");
	fprintf(stderr, "       -s: seed to use for PRNG (uses time if not specified - useful for testing)\n");
	fprintf(stderr, "       -e: run exhaustive search (for testing only)\n");
	fprintf(stderr, "       -h: print this help and exit\n");
	exit(1);
}

/* Computing the confidence levels */
static double confidence(double *conf_zeros, double *conf_comp) {
	struct compression_info *info = &comp_info_array[num_procs];
	int total_samples = info->num_zero_blocks + info->num_non_zero_blocks;
	double after_zero_perc = (((double)info->num_non_zero_blocks / total_samples) * 100);
	double after_rtc_perc = (double)info->compression_ratio * 100 / (double)info->num_non_zero_blocks;
	double estimated_var = (info->c_squared/ (double)info->num_non_zero_blocks) - pow((info->compression_ratio / (double)info->num_non_zero_blocks),2);

	/* Basic confidence from a strightforward Hoeffding bound:
	The bond is err <= sqrt(ln(2/\delta)/ (2*sample_size))
	If \delta= 10^{-7} then ln(2/\delta) <= 16.82
	If \delta= 10^{-6} then ln(2/\delta) <= 14.51
	*/
    *conf_zeros = (sqrt(16.82/(2*(double)total_samples)));
    *conf_comp = (sqrt(16.82/(2*(double)info->num_non_zero_blocks)));
    
	/* Take into account the estimated variance */
    printf("Estimated variance %f.1\n", estimated_var);
    
	return *conf_comp;
}

static void compress_chunk_random(int fd, off_t read_location, unsigned char
		*inbuf, unsigned char *outbuf, struct compression_info *info) {
	int ret;
	ssize_t bytes_read;		//return value of pread
	off_t end_of_comp_stream;	//end of compression stream
	size_t zlib_input_bytes = 0;	//total bytes passed into zlib
	size_t zlib_output_bytes = 0;	//total bytes output from zlib
	int buffer_size;
	z_stream strm;
	long int random_num;
	size_t total_read;
	unsigned char *bufptr, *tmp_ptr;
	size_t ai,saved_ai,ti,saved_ti;

//	printf("Reading location: %d \n", read_location); 

	bytes_read = pread(fd, inbuf, INBLOCK_SIZE, read_location);
	if (bytes_read == -1) {
		perror("pread");
		exit(1);
	}
	info->total_blocks_read++;

	if (is_zero_block((char *) inbuf)) {
		info->num_zero_blocks++;
		return;
	}

	info->num_non_zero_blocks++;

	random_num = random() % INBLOCK_SIZE;
	buffer_size = INBLOCK_SIZE - random_num;
	end_of_comp_stream = read_location + COMP_UNIT_SIZE + COMP_UNIT_SIZE; //+1 ?????

	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	ret = deflateInit(&strm, 1);
	if (ret != Z_OK) {
		fprintf(stderr, "Error: failed to initialize compressor\n");
		exit(1);
	}

	strm.next_out = outbuf;
	strm.avail_out = OUTBLOCK_SIZE;
	strm.next_in = inbuf + random_num;
	bufptr = inbuf + random_num;
	strm.avail_in = min((int)buffer_size, ZLIB_BLOCK_SIZE);

	do {
		saved_ti = strm.total_in;
		saved_ai = strm.avail_in;

//		printf("before deflate - a_in: %d,  a_out: %d, t_in:  %d, t_out: %d, buffer_size: %d\n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out, buffer_size );

		ret = deflate_cont(&strm, Z_SYNC_FLUSH);
		if (ret != Z_OK) {
			fprintf(stderr, "Error: failed to compress (%d)\n", ret);
			exit(1);
		}

//		ti = ai_saved - strm.avail_in;
		ti = strm.total_in;
		
//		zlib_input_bytes += ti;
//		zlib_output_bytes += strm.total_out;
		buffer_size -= (ti-saved_ti);
        bufptr += (ti-saved_ti);
//		printf("after deflate - a_in: %d,  a_out: %d, t_in:  %d, t_out: %d, buffer_size: %d\n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out, buffer_size );
		
		/* If we already filled the output buffer, we can stop */
		if (strm.avail_out == 0)
			goto done;

		if (buffer_size <= 0) {
			do {
				read_location += INBLOCK_SIZE;
				bytes_read = pread(fd, inbuf, INBLOCK_SIZE, read_location);
				if (bytes_read == -1) {
					perror("pread");
					exit(1);
				}
//				strm.next_in = inbuf;
				bufptr = inbuf;
				info->total_blocks_read++;
			} while (is_zero_block((char *) inbuf) && (read_location < end_of_comp_stream));

			if (read_location >= end_of_comp_stream) {
				goto done;
			}

			buffer_size = INBLOCK_SIZE;
		}

		strm.next_in = bufptr;
		strm.avail_in = min(buffer_size, ZLIB_BLOCK_SIZE);
	} while (strm.avail_out);

done:

	deflateEnd(&strm);
	zlib_input_bytes = strm.total_in;
	zlib_output_bytes = strm.total_out;
//	printf("total_in: %d   total out: %d ratio: %6.4f\n", zlib_input_bytes, zlib_output_bytes, (double)zlib_input_bytes/(double)zlib_output_bytes); 
	info->compression_ratio += (double)zlib_output_bytes/(double)zlib_input_bytes;
	info->c_squared += pow((double)zlib_output_bytes/(double)zlib_input_bytes,2);
}

static void compress_chunks_sequential(int fd, off_t *pattern, int
		pattern_size, unsigned char *inbuf, unsigned char *outbuf,
		struct compression_info *info)
{
	int ret;
	int index = 0;
	unsigned char *bufptr;
	ssize_t bytes_read;		//return value of pread
	size_t zlib_input_bytes = 0;	//total bytes passed into zlib
	size_t zlib_output_bytes = 0;	//total bytes output from zlib
	int buffer_size = 0;		//how much space we have in inbuf
	z_stream strm;
	int zero_blocks = 0;
	int non_zero_blocks = 0;
	int ai,saved_ai, ti,saved_ti;
	unsigned char *ni, *no;
	
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	ret = deflateInit(&strm, 1);
	if (ret != Z_OK) {
		fprintf(stderr, "Error: failed to initialize compressor\n");
		exit(1);
	}

	strm.next_out = outbuf;
	strm.avail_out = OUTBLOCK_SIZE;

//	printf("at proces start - pattern_size: %d \n",pattern_size );

	while(1) {
		/* get more data into inbuf */
		if (buffer_size <= 0) {
			while (1) {
				if (index == pattern_size)
					goto done;

				bytes_read = pread(fd, inbuf, INBLOCK_SIZE, pattern[index]);
				if (bytes_read == -1) {
					perror("pread");
					exit(1);
				}
//				info->total_blocks_read++;
				index++;

				if (is_zero_block((char *) inbuf)) {
//					info->num_zero_blocks++;
					zero_blocks++;
				} else {
					break;
				}
			}

//			info->num_non_zero_blocks++;
			non_zero_blocks++;

			buffer_size = INBLOCK_SIZE;
			strm.next_in = inbuf;
			bufptr = inbuf;
			strm.avail_in = min(buffer_size, ZLIB_BLOCK_SIZE);
			if (strm.avail_in < 1) {
				printf("careful, a_in = %d \n", strm.avail_in);
			}
		}

//		printf("before deflate - a_in: %d,  a_out: %d, t_in:  %d, t_out: %d, buffer_size: %d\n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out, buffer_size );
//		strm.total_in  = 0;
//		strm.total_out = 0;
//		strm.reserved = 0;
		
		saved_ai = strm.avail_in;
		saved_ti = strm.total_in;

//		fprintf(stderr, "before ai: %d ao: %d \n", ai, ao);
		
		ret = deflate_cont(&strm, Z_SYNC_FLUSH);
		if (ret != Z_OK) {
			fprintf(stderr, "Error: failed to compress (%s)\n", strm.msg);
			exit(1);
		}
//		ti = ai - strm.avail_in;
		ti = strm.total_in;

//		printf("after deflate - a_in: %d a_out: %d, ti: %d, t_out %d \n",strm.avail_in, strm.avail_out, ti, strm.total_out);

//		printf("total_in: %d avail_in: %d\n",  strm.total_in, strm.avail_in); 
//		printf("after deflate - a_in: %d a_out: %d, t_in: %d, t_out %d \n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out);
		
		if ((ti-saved_ti) > saved_ai) {
			fprintf(stderr, "reserved not zero\n");
			fprintf(stderr, "before ai: %d ti: %d \n", saved_ai, saved_ti);
			fprintf(stderr, "after  ai: %u ao: %u ti: %lu to: %lu res: %lu pointer: %lu\n", strm.avail_in, strm.avail_out, strm.total_in, strm.total_out, strm.reserved, strm.next_in - bufptr);
		}
//		if (ti < strm.reserved) {
//			fprintf(stderr, "warning: deflate returned total_in = %d, buffer_size = %d \n", strm.total_in, buffer_size);
//			fprintf(stderr, "before ai: %d ao: %d ti: %d to: %d \n", ai, ao,ti, to);
//			fprintf(stderr, "after  ai: %d ao: %d ti: %d to: %d res: %d pointer: %d\n", strm.avail_in, strm.avail_out, strm.total_in, strm.total_out, strm.reserved, strm.next_in - bufptr);
//		}
		buffer_size -= (ti-saved_ti);
		bufptr += (ti-saved_ti);
		strm.next_in = bufptr;

		if (strm.avail_in <= 0) {
			strm.avail_in = min((int)buffer_size, ZLIB_BLOCK_SIZE);
		}

		if (strm.avail_out <= 0) {
			zlib_input_bytes += strm.total_in;
			zlib_output_bytes += strm.total_out;
//		    printf("before reset - a_in: %d a_out: %d, t_in: %d, t_out %d \n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out);

			
			deflateReset(&strm);
//			deflateEnd(&strm);

//			strm.zalloc = Z_NULL;
//			strm.zfree = Z_NULL;
//			strm.opaque = Z_NULL;
//			ret = deflateInit(&strm, 1);
//			if (ret != Z_OK) {
//				fprintf(stderr, "Error: failed to initialize compressor\n");
//				exit(1);
//			}
			
	
			strm.next_out = outbuf;
			strm.next_in = bufptr;
			strm.avail_out = OUTBLOCK_SIZE;
			strm.avail_in = min((int)buffer_size, ZLIB_BLOCK_SIZE);
//		    printf("after reset - a_in: %d a_out: %d, t_in: %d, t_out %d \n",strm.avail_in, strm.avail_out, strm.total_in, strm.total_out);

			}

//		if (strm.avail_in < 0) {
//			printf("warning, avail_in = %d \n", strm.avail_in);
//		}
	}

done:
//	printf("at done ! \n"); 

	deflateEnd(&strm);
//	zlib_input_bytes += strm.total_in;
//	zlib_output_bytes += strm.total_out;
//	printf("total_in: %d   total out: %d non_zero: %d \n", zlib_input_bytes, zlib_output_bytes, info->num_non_zero_blocks); 
//	printf("total_in: %d   total out: %d non_zero: %d  ratio: %6.4f\n", zlib_input_bytes, zlib_output_bytes, info->num_non_zero_blocks, (double)zlib_input_bytes/(double)zlib_output_bytes); 
	if (zlib_input_bytes) {
		info->compression_ratio = (double)zlib_output_bytes/(double)zlib_input_bytes;
		info->compression_ratio *= (double) non_zero_blocks;
	}
	info->num_non_zero_blocks = non_zero_blocks;		
	info->num_zero_blocks = zero_blocks;
	info->total_blocks_read = (zero_blocks + non_zero_blocks);
}

/* The child process opens the device, reads and compresses chunks according
 * to the pattern, and calculates compression statistics. */
static void child(off_t *pattern, int pattern_size, int exhaustive, struct compression_info *info)
{
	int i;
	int fd;
	int ret;
	unsigned char *inbuf;
	unsigned char *outbuf;

	inbuf = (unsigned char *) malloc(INBLOCK_SIZE);
	if (!inbuf) {
		fprintf(stderr, "Failed to allocate memory for read buffer\n");
		exit(1);
	}

	outbuf = (unsigned char *) malloc(OUTBLOCK_SIZE);
	if (!outbuf) {
		fprintf(stderr, "Failed to allocate memory for compression buffer\n");
		exit(1);
	}

	memset(info, 0, sizeof(struct compression_info));

	fd = open(dev_name, O_RDONLY);
	if (fd == -1) {
		perror("open");
		exit(1);
	}

	if (exhaustive) {
		compress_chunks_sequential(fd, pattern, pattern_size, inbuf, outbuf, info);
	} else {
		for (i = 0; i < pattern_size; i++) {
			compress_chunk_random(fd, pattern[i], inbuf, outbuf, info);
		}
	}

	close(fd);

	exit(0);
}

/* Create a pattern of chunks for a child process to read from the device.
 * Returns the number of chunks added to the array. Adjusts the number of
 * chunks returned according to the number of active processes, so that they
 * run in a staggered fashion.*/
static int get_pattern(off_t *pattern, int exhaustive, int num_chunks, int
		active_procs, struct compression_info *info)
{
	int i = 0;
	int max_blocks;
	static int cur_chunk = 0;

	//Each process gets a consecutive chunk, which may cause seeks - optimize
	//later so that processes read more in parallel.
	if (exhaustive) {
		max_blocks = COMP_UNIT_SIZE / INBLOCK_SIZE;
		while ((i < max_blocks) && (cur_chunk < num_chunks)) {
			pattern[i] = (off_t)cur_chunk * INBLOCK_SIZE;
			cur_chunk++;
			i++;
		}
	} else {
		max_blocks = ((double)(active_procs+1)/(double)num_procs) * BLOCKS_PER_PROC;
		if (max_blocks > BLOCKS_PER_PROC)
			max_blocks = BLOCKS_PER_PROC;

		if ((info->num_non_zero_blocks >= MAX_NUM_SAMPLE) || (info->num_zero_blocks >= (MAX_NUM_SAMPLE * ZERO_BLOCK_FACTOR)))
			return 0;
		while (i < max_blocks) {
			pattern[i] = (off_t)(random() % num_chunks) * INBLOCK_SIZE;
			i++;
		}
	}

	return i;
}

/* Get an unused slot in the PID array */
static int get_empty_pid_index()
{
	int i;
	for (i = 0; i < num_procs; i++) {
		if (pid_array[i] == 0)
			return i;
	}
	fprintf(stderr, "Error: Did not find any empty pid slots in array!\n");
	return -1;
}

/* Wait for a child process to exit, and then aggregate its results */
static int wait_for_process()
{
	int i;
	pid_t ret;
	int status;

	do {
		ret = wait(&status);
	} while (ret == -1);
	
	if (!WIFEXITED(status)) {
		fprintf(stderr, "process %d exited abnormally !! \n", ret);
		return -1;
	}

	for (i = 0; i < num_procs; i++) {
		if (pid_array[i] == ret) {
			pid_array[i] = 0;
			comp_info_array[num_procs].num_zero_blocks += comp_info_array[i].num_zero_blocks;
			comp_info_array[num_procs].num_non_zero_blocks += comp_info_array[i].num_non_zero_blocks;
			comp_info_array[num_procs].total_blocks_read += comp_info_array[i].total_blocks_read;
			comp_info_array[num_procs].compression_ratio += comp_info_array[i].compression_ratio;
			comp_info_array[num_procs].c_squared += comp_info_array[i].c_squared;
			return 0;
		}
	}
	fprintf(stderr, "Error: Did not find pid in array!\n");
	return -1;
}

/* Subtract two timeval structures and return the difference */
static double timeval_subtract(struct timeval *x, struct timeval *y)
{
	struct timeval result;

	/* Perform the carry for the later subtraction by updating y. */
	if (x->tv_usec < y->tv_usec) {
		int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
		y->tv_usec -= 1000000 * nsec;
		y->tv_sec += nsec;
	}
	if (x->tv_usec - y->tv_usec > 1000000) {
		int nsec = (x->tv_usec - y->tv_usec) / 1000000;
		y->tv_usec += 1000000 * nsec;
		y->tv_sec -= nsec;
	}

	result.tv_sec = x->tv_sec - y->tv_sec;
	result.tv_usec = x->tv_usec - y->tv_usec;

	return (double)result.tv_sec + ((double)result.tv_usec / 1000000);
}

/* Print the aggregated statistics */
static void print_status(int final)
{
	struct compression_info *info = &comp_info_array[num_procs];
	char csv_output[MAX_STRING_LEN];
	double dev_size_mb = (double)dev_size / 1048576;
	int total_samples = info->num_zero_blocks + info->num_non_zero_blocks;
	double after_zero_size = (((double)info->num_non_zero_blocks / total_samples) * dev_size_mb);
	double after_zero_perc = (((double)info->num_non_zero_blocks / total_samples) * 100);
	double after_rtc_size = (info->compression_ratio * after_zero_size) / (double)info->num_non_zero_blocks;
	double after_rtc_perc = (double)info->compression_ratio * 100 / (double)info->num_non_zero_blocks;
	double conf_zeros;
	double conf_comp;
		
	double error = (after_zero_size * confidence(&conf_zeros,&conf_comp));

	memset(csv_output, 0, MAX_STRING_LEN);
	snprintf(csv_output, (MAX_STRING_LEN-1), "%d, %d, %d, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f,%.3f, %.3f\n",
			info->num_zero_blocks, info->num_non_zero_blocks, info->total_blocks_read, info->compression_ratio, conf_comp,
			dev_size_mb, after_zero_size, after_zero_perc, conf_zeros, after_rtc_size, after_rtc_perc, error);

	if (final && res_file) {
		fprintf(res_file, csv_output);
		fflush(res_file);
		return;
	}

    fprintf(stderr, "Based on %d samples, %d non-zero\n", total_samples, info->num_non_zero_blocks);
	fprintf(stderr, "%.2f%% Non-zero percent (+- %.2f%%) - Volume after migration (w/o RTC): %.1f MB\n", after_zero_perc, conf_zeros*100.0, after_zero_size);
	fprintf(stderr, "%.2f%% Compression rate (+- %.2f%%) - Volume after migration (with RTC): %.1f MB\n", after_rtc_perc, conf_comp*100.0, after_rtc_size);
	fprintf(stderr, "**************************************************\n");
	
	
//	fprintf(stderr, "Full volume: %.1f MB\n", dev_size_mb);
//	fprintf(stderr, "Volume after migration (w/o RTC): %.1f MB (%.1f%% non-zero blocks)\n", after_zero_size, after_zero_perc);
//	fprintf(stderr, "Volume after migration (with RTC): %.1f MB (%.1f%%)\n", after_rtc_size, after_rtc_perc);
//	fprintf(stderr, "Error estimation: +/- %.3f MB\n", error);
//	fprintf(stderr, "**************************************************\n");
//	fflush(stderr);

	if (csv_file) {
		fprintf(csv_file, csv_output);
		fflush(csv_file);
	}
}

/* Clean up everything in case we exit regularly (signum==0) or get a signal to
 * exit */
static void cleanup_handler(int signum)
{
	int i;

	time_t end_time = time(NULL);
	time_t tot_time = time(NULL) - start_time;
	fprintf(stderr, "Total run time: %ld seconds\n", tot_time);

	if (res_file) {
		fprintf(res_file, ", %.2f, ", tot_time);
		print_status(1);
	}

	if (comp_info_array)
		munmap(comp_info_array, shared_mem_size);

	for (i = 0; i < num_procs; i++) {
		if (pid_array[i])
			kill(pid_array[i], SIGKILL);
	}

	if (signum)
		exit(signum);
}

int init_log_files(char *log_name, char *csv_name, char *res_name, int exhaustive)
{
	int ret;
	struct tm *ltime;
	char csv_output[MAX_STRING_LEN];
	time_t start_seconds = 0;
	double dev_size_mb = (double)dev_size / 1048576;

	if (log_name) {
		log_file = fopen(log_name, "a");
		if (!log_file) {
			perror("open(log file)");
			return errno;
		}
		ret = dup2(fileno(log_file), fileno(stderr));
		if (ret == -1) {
			perror("dup2");
			return errno;
		}
	}

	if (csv_name) {
		csv_file = fopen(csv_name, "a");
		if (!csv_file) {
			perror("open(csv file)");
			return errno;
		}
		ret = dup2(fileno(csv_file), fileno(stdout));
		if (ret == -1) {
			perror("dup2");
			return errno;
		}
	}

	if (res_name) {
		res_file = fopen(res_name, "a");
		if (!res_file) {
			perror("open(res file)");
			return errno;
		}
	}

	time(&start_seconds);
	ltime = localtime(&start_seconds);

	fprintf(stderr, "Start time: %02d/%02d/%4d %02d:%02d:%02d\n", ltime->tm_mday, (ltime->tm_mon+1), (ltime->tm_year+1900), ltime->tm_hour, ltime->tm_min, ltime->tm_sec);
	fprintf(stderr, "Device name: %s\n", dev_name);
	fprintf(stderr, "Device size: %.1f MB\n", dev_size_mb);
	fprintf(stderr, "Number of processes: %d\n", num_procs);
	fprintf(stderr, "Exhaustive: %s\n", (exhaustive ? "yes" : "no"));
	fprintf(stderr, "\n");

	memset(csv_output, 0, MAX_STRING_LEN);
	snprintf(csv_output, (MAX_STRING_LEN-1), "%02d/%02d/%4d %02d:%02d:%02d, %s, %.1f, %d, %s", ltime->tm_mday, (ltime->tm_mon+1), (ltime->tm_year+1900), ltime->tm_hour, ltime->tm_min, ltime->tm_sec, dev_name, dev_size_mb, num_procs, (exhaustive ? "yes" : "no"));
	if (csv_file) {
		fprintf(csv_file, csv_output);
		fprintf(csv_file, "\n");
		fflush(csv_file);
	}
	if (res_file) {
		fprintf(res_file, csv_output);
		fflush(res_file);
	}
	fflush(stderr);

	return 0;
}

int main(int argc, char **argv)
{
	int c;
	int ret = 0;
	int index;
	int num_chunks;
	int exhaustive = 0;
	int active_procs = 0;
	char *log_name = NULL;
	char *csv_name = NULL;
	char *res_name = NULL;
	unsigned int seed;
	unsigned int seed_set = 0;
	int pattern_size;
	off_t *pattern = NULL;

	signal(SIGINT, cleanup_handler);
	signal(SIGTERM, cleanup_handler);
	signal(SIGHUP, cleanup_handler);

	while ((c = getopt (argc, argv, "d:p:l:c:r:s:eh")) != -1)
		switch (c)
		{
			case 'd':
				dev_name = optarg;
				break;
			case 'p':
				num_procs = atoi(optarg);
				break;
			case 'l':
				log_name = optarg;
				break;
			case 'c':
				csv_name = optarg;
				break;
			case 'r':
				res_name = optarg;
				break;
			case 's':
				seed = atoi(optarg);
				seed_set = 1;
				break;
			case 'e':
				exhaustive = 1;
				break;

			case 'h':
				usage(argv[0]);
				break;
			case '?':
				fprintf (stderr, "Unknown option `-%c'.\n", optopt);
				return 1;
			default:
				return 1;
		}

	if (!dev_name)
		usage(argv[0]);

	if ((num_procs < 0) || (num_procs > MAX_NUM_PROCS)) {
		fprintf(stderr, "Number of processes should be between 0 and %d.\n", MAX_NUM_PROCS);
		usage(argv[0]);
	}

	shared_mem_size = sizeof(struct compression_info) * (num_procs + 1);
	comp_info_array = (struct compression_info *) mmap(NULL, shared_mem_size, PROT_READ | PROT_WRITE,
			MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if (comp_info_array == (void *)-1) {
		perror("mmap");
		comp_info_array = NULL;
		ret = errno;
		goto out;
	}

	memset(comp_info_array, 0, shared_mem_size);
	memset(pid_array, 0, sizeof(pid_t) * MAX_NUM_PROCS);

	if (seed_set)
		srandom(seed);
	else
		srandom((unsigned int)time(NULL));

	if (ret)
		goto out;
	
	dev_size = get_dev_size();
	num_chunks = (int)(dev_size / INBLOCK_SIZE); //1MB size gives 2PB on 32-bit system

	if (num_chunks < 1) {
		fprintf(stderr, "Error: device size is too small\n");
		goto out;
	}

	if (exhaustive)
		pattern = (off_t *) malloc(sizeof(off_t) * (COMP_UNIT_SIZE / INBLOCK_SIZE));
	else
		pattern = (off_t *) malloc(sizeof(off_t) * BLOCKS_PER_PROC);

	ret = init_log_files(log_name, csv_name, res_name, exhaustive);

	start_time = time(NULL);

	while ((pattern_size = get_pattern(pattern, exhaustive, num_chunks, active_procs, &comp_info_array[num_procs])))
	{
		debug_print("active: %d, total: %d\n", active_procs, num_procs);
		if (active_procs >= num_procs) {
			ret = wait_for_process();
			if (ret == -1)
				goto out;
			print_status(0);
			active_procs--;
		}
		index = get_empty_pid_index();
		if (index == -1) {
			ret = -1;
			goto out;
		}
		pid_array[index] = fork();
		if (pid_array[index] == -1) {
			perror("fork");
			ret = errno;
			goto out;
		} else if (pid_array[index] == 0) {
			child(pattern, pattern_size, exhaustive, &comp_info_array[index]);
		}
		active_procs++;
	}

	while (active_procs) {
		ret = wait_for_process();
		if (ret == -1)
			goto out;
		print_status(0);
		active_procs--;
	}

out:
	if (pattern)
		free(pattern);
	cleanup_handler(0);
	return ret;
}

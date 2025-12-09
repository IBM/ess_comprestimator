from enum import Enum
import subprocess
import argparse
import os
import random
import tempfile
import tarfile
import csv

DEFAULT_SAMPLE_FILE_SIZE = 10_000_000_000 # If weighted sampling, sets max file size of sample archive
SIMPLE_SAMPLE_FILE_COUNT = 10 # If simple sampling, sets max file count
DEFAULT_SAMPLING_PERCENTAGE = .1

COMPRESTIMATOR_PATH = "./comprestimator"
COMPRESTIMATOR_RESULTS_PATH = "./results.csv"

KNOWN_COMPRESSED_FILE_SUFFIXES = [".png", ".jpg", ".jpeg", ".gif", ".mp3", ".mp4", ".docx", ".xlsx", ".zip", ".rar", ".bz", ".gz"]

class SamplingStrategy(Enum):
    AUTO = 0
    EXHAUSTIVE = 1  # Doesn't sample, takes entire directory (most accurate, slow)
    WEIGHTED = 3    # Samples weighted on file size, (good accuracy and speed)

class Sampler():
    def __init__(self, max_file_size = DEFAULT_SAMPLE_FILE_SIZE, sample_file_count = SIMPLE_SAMPLE_FILE_COUNT):
        self.max_file_size = max_file_size
        self.sample_file_count = sample_file_count

    def sample(self, strategy: SamplingStrategy, file_size_list: list[tuple[str, int]], total_dir_size: int) -> list[str]:
        """
        Given a sampling strategy and a list of (path, file_size) tuples, returns sampled list of file paths
        """

        # if weighted and directory is smaller than max output file size, it's equivalent to exhaustive
        if strategy == SamplingStrategy.AUTO or strategy == SamplingStrategy.WEIGHTED:
            if total_dir_size <= self.max_file_size: # dir is smaller than max, just do exhaustive
                print("Directory is small, so switching to EXHAUSTIVE strategy")
                strategy = SamplingStrategy.EXHAUSTIVE
            else:
                strategy = SamplingStrategy.WEIGHTED

        print(f"Using {strategy.name} sampling strategy ")

        if strategy == SamplingStrategy.EXHAUSTIVE: # returns initial list with sizes removed
            return self.exhaustive_sample(file_size_list) 
        else:
            return self.weighted_sample(file_size_list, total_dir_size)
            
    def exhaustive_sample(self, file_size_list: list[tuple[str, int]]) -> list[str]:
        paths, _ = zip(*file_size_list)
        return list(paths)
        
    def weighted_sample(self, file_size_list: list[tuple[str, int]], total_dir_size) -> list[str]:
        """
        Weighted sample based on file sizes
        """
        current_size = 0
        failed_attempts = 0
        sampling_list: list[str] = []
        while current_size < self.max_file_size:
            start_size = len(sampling_list)
            random_point = random.randrange(0,total_dir_size-current_size)
            current_point = 0
            point_to_remove = None
            for entry in file_size_list:
                file, size = entry
                current_point += size
                if random_point < current_point:
                    sampling_list.append(file)
                    print(f"Added {file} with size {size} so current archive is {current_size+size}")
                    current_size += size
                    point_to_remove = entry
                    break
            if point_to_remove:
                print(f"Removing {point_to_remove} from consideration for further sampling")
                file_size_list.remove(point_to_remove)
            if len(file_size_list) == 0:
                break
            if start_size == len(sampling_list):
                failed_attempts += 1
            if failed_attempts >= 3:
                print("Note: Several failed sampling attempts. Results may be inaccurate")
                break
            
        if len(sampling_list) == 0:
            raise Exception("Could not sample with provided percentage! Try a different percentage or use an exhaustive sammple")

            
        return sampling_list


def validate_path(path: str) -> str:
    """
    Checks if a given path is a valid directory or file
    """
    if not os.path.isdir(path) and not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"'{path}' does not exist as a file or directory")
    return path


def percent_type(value):
    if not value.endswith('%'):
        raise argparse.ArgumentTypeError("Percentage must end with '%'")
    try:
        percentage = float(value[:-1])
        if not (0 <= percentage <= 100):
            raise argparse.ArgumentTypeError("Percentage must be between 0 and 100")
        return percentage/100
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid percentage value")


def file_comprestimator(input_path: str):
    comprestimator_exists = os.path.isfile(COMPRESTIMATOR_PATH)
    if not comprestimator_exists:
        raise FileNotFoundError(f"""Comprestimator executable not found  at {COMPRESTIMATOR_PATH}.  
                                    Make sure you are running this python file from same directory as the comprestimator executable""")
    _ = subprocess.run(["./comprestimator", "-d", input_path, "-r", COMPRESTIMATOR_RESULTS_PATH], check=True)
    print(f"Comprestimator ran successfully, wrote results to {COMPRESTIMATOR_RESULTS_PATH}")


def check_if_compressed(file: str, found_compressed_types: set):
    for ext in KNOWN_COMPRESSED_FILE_SUFFIXES:
        if file.endswith(ext):
            found_compressed_types.add(ext)
            return


def directory_comprestimator(src_dir: str, sampling_strategy=SamplingStrategy.AUTO, sampling_percentage=None, skip_nested_directories=False) -> str:
    """
    Given a directory path, randomly samples files and creates a tar archive from them, and then runs comprestimator on the archive
    """
    files_with_sizes: list[tuple[str, int]] = []
    total_size = 0

    # Walk directory, collecting files and their sizes
    if skip_nested_directories:
        print("Skipping nested directories...")
        for path in os.listdir(src_dir):
            full_path = os.path.join(src_dir, path)
            if os.path.isfile(full_path):
                try:
                    file_size = os.lstat(full_path).st_size
                    files_with_sizes.append((full_path, file_size))
                    total_size += file_size
                except OSError:
                    continue
    else:
        for root, _, files in os.walk(src_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                try:
                    file_size = os.lstat(file_path).st_size
                    files_with_sizes.append((file_path, file_size))
                    total_size += file_size
                except OSError:
                    continue



    if len(files_with_sizes) == 0 or total_size == 0:
        raise Exception("Directory is empty or all files are empty!")
    print(f"Directory has {len(files_with_sizes)} files totalling {total_size} bytes. Sampling files to create archive file...")

    # create list of files to archive
    sample_size = DEFAULT_SAMPLE_FILE_SIZE
    if sampling_strategy == SamplingStrategy.EXHAUSTIVE: # exhaustive, sample everything
        sample_size = total_size
    elif sampling_percentage is not None: # provided sampling percentage, so sample that much
        sample_size = int(sampling_percentage * total_size)
    else: # not exhaustive, no percentage provided 
        # files over 10GB -> sample at least 10GB
        sample_size = max(DEFAULT_SAMPLE_FILE_SIZE, int(DEFAULT_SAMPLING_PERCENTAGE * total_size))
    
    print(f"Sampling {sample_size} bytes from directory")

    sampler = Sampler(max_file_size=sample_size)        
    files_sample = sampler.sample(sampling_strategy, files_with_sizes, total_size)

    print("Creating archive for comprestimator input...")

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        # add sample files to archive
        with tarfile.open(fileobj=temp_file, mode='w:') as tar:
            for file in files_sample:
                tar.add(file)
        
        print("Running comprestimator on archive...")

        # close archive and run comprestimator on it
        temp_file.close()
        file_comprestimator(temp_file.name)        

        print("Comprestimator finished!")
    finally: 
        # delete archive
        os.unlink(temp_file.name)


def main():
    parser = argparse.ArgumentParser(description="Estimates FCM Compression on GPFS for a given input file/directory")
    parser.add_argument('-p', '--path', type=validate_path, required=True, help="Path to input file/directory")  # path to process
    sampling_args = parser.add_mutually_exclusive_group()
    sampling_args.add_argument('--exhaustive-sampling', action="store_true", help="Samples entire input directory for greatest accuracy. Note this will be slow on large directories")
    sampling_args.add_argument('--sampling-percentage', type=percent_type, default=None, help="Percentage of input directory size to sample (e.g. 10%%). Increasing this percentage will increase accuracy but slow down the tool.")
    parser.add_argument('--skip-nested-directories', action="store_true", help="Will not sample directories nested within target directory, only files")
    args = parser.parse_args()
    input_path = args.path

    skip_nested_directories = False
    sampling_strategy = SamplingStrategy.AUTO
    sampling_percentage = None

    if args.skip_nested_directories:
        skip_nested_directories = True

    if args.exhaustive_sampling:
        sampling_strategy = SamplingStrategy.EXHAUSTIVE
    elif args.sampling_percentage is not None:
        sampling_percentage = args.sampling_percentage

    path_is_a_directory = os.path.isdir(input_path)
    if path_is_a_directory:
        # If input is a directory, convert it to a file and run comprestimator on that
        print(f"'{input_path}' is a directory, sampling to create an input file for comprestimator. This may take a while for deeply nested directories...")
        directory_comprestimator(input_path, sampling_strategy, sampling_percentage, skip_nested_directories)
    else:
        # If input is a file, just run comprestimator on it directly
        print(f"'{input_path}' is a file, sampling directly with comprestimator...")
        file_comprestimator(input_path)

    # Extract results from comprestimator results file and print them
    with open(COMPRESTIMATOR_RESULTS_PATH, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)
        if not data:
            raise Exception("Comprestimator results file empty!")
        
        most_recent_results = data[-1]
        initial_size = float(most_recent_results[12])
        compressed_size = float(most_recent_results[15])
        
    # Print final results
    print()
    print("*" * 20)
    print("Comprestimator Results:")
    print(f"Pre-compression sample size  : {initial_size}")
    print(f"Post-compression sample size : {compressed_size}")
    if compressed_size == 0:
        print("Error! Post-compression size is 0; Cannot get compression ratio")
    else:
        print(f"Estimated Compression Ratio  : {initial_size/compressed_size}x")


if __name__ == "__main__":
    main()
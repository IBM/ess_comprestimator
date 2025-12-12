from enum import Enum
import subprocess
import argparse
import os
import random
import tempfile
import tarfile
import csv
from fnmatch import fnmatch

DEFAULT_SAMPLE_FILE_SIZE = 10_000_000_000 # If weighted sampling, sets max file size of sample archive
DEFAULT_SAMPLING_PERCENTAGE = .1

COMPRESTIMATOR_PATH = "./comprestimator"
COMPRESTIMATOR_RESULTS_PATH = "./results.csv"

KNOWN_COMPRESSED_FILE_SUFFIXES = [".png", ".jpg", ".jpeg", ".gif", ".mp3", ".mp4", ".docx", ".xlsx", ".zip", ".rar", ".bz", ".gz"]

class SamplingStrategy(Enum):
    AUTO = 0
    EXHAUSTIVE = 1  # Doesn't sample, takes entire directory (most accurate, slow)
    WEIGHTED = 3    # Samples weighted on file size, (good accuracy and speed)

class Sampler():
    def __init__(self, max_file_size = DEFAULT_SAMPLE_FILE_SIZE):
        self.max_file_size = max_file_size

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
            initial_list_size = len(sampling_list)
            random_point = random.randrange(0,total_dir_size-current_size)
            current_point = 0
            entry_to_remove = None
            for entry in file_size_list:
                file, size = entry
                current_point += size
                if random_point < current_point:
                    sampling_list.append(file)
                    print(f"Added {file} with size {size} ; current archive is {current_size+size}")
                    current_size += size
                    entry_to_remove = entry
                    break
            if entry_to_remove:
                print(f"Removing {entry_to_remove} from consideration for further sampling")
                file_size_list.remove(entry_to_remove)
            if len(file_size_list) == 0:
                break
            if initial_list_size == len(sampling_list):
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
                                    Make sure you are running this python file from same directory as the comprestimator executable (and that you have compiled the executable with 'make')""")
    _ = subprocess.run(["./comprestimator", "-d", input_path, "-r", COMPRESTIMATOR_RESULTS_PATH], check=True)
    print(f"Comprestimator ran successfully, wrote results to {COMPRESTIMATOR_RESULTS_PATH}")


def check_if_compressed(file: str, found_compressed_types: set):
    for ext in KNOWN_COMPRESSED_FILE_SUFFIXES:
        if file.endswith(ext):
            found_compressed_types.add(ext)
            return


def try_adding_file(file_path, files_with_sizes):
    if not os.path.isfile(file_path):
        return 0
    try:
        file_size = os.lstat(file_path).st_size
        files_with_sizes.append((file_path, file_size))
        return file_size
    except OSError:
        print(f"OSError when adding file {file_path} for consideration")
        return 0


def is_excluded(root, file_name, excluded_patterns, skip_hidden):
    full_path = os.path.join(root,file_name)
    should_hide = (skip_hidden and file_name.startswith(".")) or \
        any((fnmatch(full_path, pattern) or \
             fnmatch(file_name, pattern)) for pattern in excluded_patterns)
    if should_hide:
        print(full_path, "is excluded, won't evaluate")
    return should_hide

def directory_comprestimator(src_dir: str, sampling_strategy=SamplingStrategy.AUTO, sampling_percentage=None, skip_nested_directories=False, excluded_patterns=[], skip_hidden=False) -> str:
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
            if not is_excluded(src_dir, path, excluded_patterns, skip_hidden):
                total_size += try_adding_file(full_path, files_with_sizes)
    else:
        for root, dirs, files in os.walk(src_dir):
            # exclude paths we dont want
            dirs[:] = [d for d in dirs if not is_excluded(root, d, excluded_patterns, skip_hidden)]
            files[:] = [f for f in files if not is_excluded(root, f, excluded_patterns, skip_hidden)]

            # for remaining files, consider them for sampling
            for file_name in files:
                file_path = os.path.join(root, file_name)
                total_size += try_adding_file(file_path, files_with_sizes)

    if len(files_with_sizes) == 0 or total_size == 0:
        raise Exception("Directory is empty or all files are empty!")
    print(f"Directory has {len(files_with_sizes)} files totalling {total_size} bytes. Sampling files to create archive file...")

    # create list of files to archive
    sample_size = DEFAULT_SAMPLE_FILE_SIZE
    if sampling_strategy == SamplingStrategy.EXHAUSTIVE: # exhaustive, sample everything
        sample_size = total_size
    elif sampling_percentage is not None: # provided sampling percentage, so sample that much
        sample_size = int(sampling_percentage * total_size)
    else: # not exhaustive, no percentage provided (default behavior to sample 10%)
        sample_size = max(DEFAULT_SAMPLE_FILE_SIZE, int(DEFAULT_SAMPLING_PERCENTAGE * total_size))
    
    print(f"Sampling {sample_size} bytes from directory")

    sampler = Sampler(max_file_size=sample_size)        
    files_sample = sampler.sample(sampling_strategy, files_with_sizes, total_size)

    print("Creating archive for comprestimator input...")

    current_dir = os.getcwd()
    temp_file = tempfile.NamedTemporaryFile(delete=False, prefix="tmp_", dir=current_dir)
    files_added = 0
    try:
        print("Creating temporary file for archive at", temp_file.name, "...")
        # add sample files to archive
        with tarfile.open(fileobj=temp_file, mode='w:') as tar:
            for file in files_sample:
                try:
                    tar.add(file)
                    files_added +=1
                except PermissionError:
                    print(f"Could not access {file} due to insufficient permissions. Skipping...")

        files_not_added = len(files_sample) - files_added
        if files_not_added >= (.25 * len(files_sample)):
            print("Warning! > 25%% of files in the sample could not be read due to lack of permissions. Comprestimator result may be inaccurate")

        print("Running comprestimator on archive...")

        # close archive and run comprestimator on it
        temp_file.close()
        file_comprestimator(temp_file.name)        

        print("Comprestimator finished!")
    finally: 
        # delete archive
        print("Deleting temporary file...")
        os.unlink(temp_file.name)


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Estimates FCM Compression on GPFS for a given input file/directory")
    parser.add_argument('-p', '--path', type=validate_path, required=True, help="Path to input file/directory")  # path to process
    sampling_args = parser.add_mutually_exclusive_group()
    sampling_args.add_argument('--exhaustive-sampling', action="store_true", help="Samples entire input directory for greatest accuracy. Note this will be slow on large directories")
    sampling_args.add_argument('--sampling-percentage', type=percent_type, default=None, help="Percentage of input directory size to sample (e.g. 10%%). Increasing this percentage will increase accuracy but slow down the tool.")
    parser.add_argument(
        '--exclude',
        metavar='FILE',
        type=str,
        nargs='*',
        default=[],
        help='File/Directory names to exclude (can be used multiple times to exclude multiple files)'
    )
    parser.add_argument('--skip-nested-directories', action="store_true", help="Will not sample directories nested within target directory, only files")
    parser.add_argument('--skip-hidden', action="store_true", help="Will not sample hidden directories and files within the target directory")
    args = parser.parse_args()
    input_path = args.path

    skip_nested_directories = False
    sampling_strategy = SamplingStrategy.AUTO
    sampling_percentage = None
    skip_hidden = args.skip_hidden

    if args.skip_nested_directories:
        skip_nested_directories = True

    excluded_patterns = args.exclude


    # Handle mutually exclusive arguments: sample a % of directory or the entire thing
    if args.exhaustive_sampling:
        sampling_strategy = SamplingStrategy.EXHAUSTIVE
    elif args.sampling_percentage is not None:
        sampling_percentage = args.sampling_percentage

    # Run Comprestimator on file or directory
    path_is_a_directory = os.path.isdir(input_path)
    if path_is_a_directory:
        # If input is a directory, convert it to a file and run comprestimator on that
        print(f"'{input_path}' is a directory, sampling to create an input file for comprestimator. This may take a while for deeply nested directories...")
        directory_comprestimator(input_path, sampling_strategy, sampling_percentage, skip_nested_directories, excluded_patterns, skip_hidden)
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
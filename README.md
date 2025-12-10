Originally authored by _Avishay Traeger_, _Danny Harnik_, _Dmitry Sotnikov_
, Modified for ESS by _Sarvesh Chezhian_


## Requirements
* x86_64 Linux
* Python3.x Installed and available in PATH

## Checkout
* Create your SSH keys, and add them to your GitHub profile.
* Checkout the repo using the SSH method (preferred)
* Should work on RHEL

## Build
```
make
```
This will generate a `comprestimator` binary.

## Execution
Locate any given input path for a file or directory in your system and run:
```
python3 run_comprestimator.py --path <file path>
```

The tool should run and output a compression ratio for the given path.

## Flags
You can run comprestimator on every file in a directory using the exhaustive sampling
flag. This will provide the greatest accuracy, though it can be slow on large directories:
```
python3 run_comprestimator.py --path <file path> --exhaustive-sampling
```


If you want fine-grained control of sampling percentage, you can use the sampling percentage
flag:
```
python3 run_comprestimator.py --path <file path> --sampling-percentage 50%
```

A higher sampling percentage will be more accurate but slower, 
and a lower sampling percentage will be less accurate but faster


By default, comprestimator will recursively evaluate every directory and file contained in 
your target directory. If you wish to exclude hidden files and folders from consideration,
you can set the skip hidden flag
```
python3 run_comprestimator.py --path <file path> --skip-hidden
```


By default, comprestimator will recursively evaluate every directory and file contained in 
your target directory. If you wish to exclude specific files and folders from consideration,
you can list them (space separated) with the --exclude flag
```
python3 run_comprestimator.py --path <file path> --exclude [FILE_1 ...]
```


By default, comprestimator will recursively explore nested directories within your target
directory to most accurately estimate compression. If you prefer scanning only the first
level of your target directory, you can use the skip nested directories flag:
```
python3 run_comprestimator.py --path <file path> --skip-nested-directories
```


Originally authored by _Avishay Traeger_, _Danny Harnik_, _Dmitry Sotnikov_

---

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
Locate any block device in your system. And run
```
./comprestimator -d <block device path>
```

It should run and output a compression ratio.

We are working to make it work for a directory tree.


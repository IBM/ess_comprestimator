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

### For Directory
We can create a loopback device on our system, format it with some filesystem, mount it on our machines, copy the data of the directory over the mounted path, and run the comprestimator for that block device.

- Create a filesystem from a file.
  `dd if=/dev/zero of=fs.img bs=10M count=1024`
  Creates a file of 10G with zeroes
- Format the file with some filesystem.
  `mkfs.ext4 fs.img`
- Mount the file as a loopback device.
  `mount -o loop fs.img /mnt/my-filesystem`
- Copy the content you want to run `comprestimator` on
  `cp -r /usr /mnt/my-filesystem`
- Run `comprestimator`
  `./comprestimator -d /dev/loop0 -r res.csv`

It should run the comprestimator on the block device and output an estimated compression ratio.


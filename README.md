# Remote Snapshotter (with [stargz format introduced by CRFS](https://github.com/google/crfs))

Related discussion of the snapshotter:
- [Support remote snapshotter to speed up image pulling#3731@containerd](https://github.com/containerd/containerd/issues/3731)
- [Support `Prepare` for existing snapshots in Snapshotter interface#2968@containerd](https://github.com/containerd/containerd/issues/2968)
- [remote filesystem snapshotter#2943@containerd](https://github.com/containerd/containerd/issues/2943)

This is an example implementation of a *remote snapshotter* which can be plugged into [patched version of containerd](https://github.com/ktock/containerd/tree/remote-snapshotter).
By using this snapshotter, any converted but docker-compatible image can be pulled in several seconds even if the images are huge.
```
# time ctr images pull --plain-http --remote-snapshot --snapshotter=remote registry2:5000/fedora:30 > /dev/null 
real	0m0.447s
user	0m0.081s
sys	0m0.019s
# time ctr images pull --plain-http --remote-snapshot --snapshotter=remote registry2:5000/python:3.7 > /dev/null 
real	0m1.041s
user	0m0.073s
sys	0m0.028s
# time ctr images pull --plain-http --remote-snapshot --snapshotter=remote registry2:5000/jenkins:2.60.3 > /dev/null 
real	0m1.231s
user	0m0.112s
sys	0m0.008s
```
To achive that we are using [stargz format introduced by CRFS](https://github.com/google/crfs), which is compatible with current docker image format.

## demo

__NOTICE:__

- Put this repo on your GOPATH(${GOPATH}/src/github.com/ktock/remote-snapshotter).

### Build and run the environment
```
$ cd ${GOPATH}/src/github.com/ktock/remote-snapshotter/demo
$ docker-compose build --build-arg HTTP_PROXY=$HTTP_PROXY \
                       --build-arg HTTPS_PROXY=$HTTP_PROXY \
                       --build-arg http_proxy=$HTTP_PROXY \
                       --build-arg https_proxy=$HTTP_PROXY \
                       containerd
$ docker-compose up -d
$ docker exec -it containerd /bin/bash
(inside container) # /run.sh
```

### Prepare stargz-formatted image on a registry

Use [stargzify](https://github.com/google/crfs/tree/master/stargz/stargzify) command to convert the image.
```
# stargzify -insecure ubuntu:18.04 http://registry2:5000/ubuntu:18.04
```
The converted image is still __compatible with a normal docker image__ so you can still pull and run it with a normal tools like docker.

### Pull the image without downloading layers(it's sometimes called "lazypull") and run it
```
# time ctr images pull --plain-http --remote-snapshot --snapshotter=remote registry2:5000/ubuntu:18.04
(Layer downloads don't occur. So this "pull" operation will end in around 1 sec.)
real	0m0.248s
user	0m0.020s
sys	0m0.011s
# ctr run --snapshotter=remote registry2:5000/ubuntu:18.04 test /bin/bash
ls
ls
bin
boot
dev
etc
...
```

### Cleanup
```
# ctr t kill -s 9 test
# ctr c rm test
```

# TODO

## General issues:
- [ ] Completing necessary patches on the containerd.
- [ ] Contributing CRFS to make it more stable.

## Snapshotter specific issues:
- [ ] Resiliency: Ensure all mounts are available on every Prepare() and report erros when unavailable.
- [ ] Auth: Implement auth-related things and the credential management(under the discussion on [#3731@containerd](https://github.com/containerd/containerd/issues/3731))
- [ ] Performance: READ performance improvement
- [ ] Availability: Especially on NW disconnection

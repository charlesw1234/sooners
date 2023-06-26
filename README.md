To build the running environment:

```sh
$ tar xJf Python-3.11.3.tar.xz
$ mkdir build-py3
$ cd build-py3
$ ../Python-3.11.3/configure --prefix=/opt/py3.sooners --enable-shared --enable-optimizations --with-ensurepip=install --enable-ipv6
$ sudo make install
```

```sh
$ cd ~/works
$ git clone https://github.com/charlesw1234/sooners.git
$ cd sooners
$ ./pip.sh install -r requirements.txt
```

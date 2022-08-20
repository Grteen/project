编译环境 Ubuntu 22.04
make main 进行编译
./main & 服务端
netstat -lnp | grep ./main 找到绑定的端口
./client 端口

#!/bin/bash

echo "=== 系统信息收集脚本 ==="
echo

echo ">>> 操作系统信息"
uname -a
echo
cat /etc/os-release
echo

echo ">>> CPU 信息"
echo "处理器数量：$(nproc)"
echo "CPU型号：$(grep "model name" /proc/cpuinfo | head -n1 | cut -d: -f2)"
echo

echo ">>> 内存信息"
free -h
echo

echo ">>> 磁盘使用情况"
df -h
echo

echo "=== 信息收集完成 ==="

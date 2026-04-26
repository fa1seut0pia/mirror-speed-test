# 镜像测速

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-22C55E.svg)](./LICENSE)
[![Release](https://img.shields.io/github/v/release/fa1seut0pia/mirror-speed-test?label=Release)](https://github.com/fa1seut0pia/mirror-speed-test/releases)
[![Release Binaries](https://github.com/fa1seut0pia/mirror-speed-test/actions/workflows/release.yml/badge.svg)](https://github.com/fa1seut0pia/mirror-speed-test/actions/workflows/release.yml)

<p align="center">
  <img src="./assert/image.png" alt="项目截图" width="720" />
</p>

一个面向开发环境的镜像源测速与配置工具，支持常见镜像的延迟、 TTFB 和下载速度测试，可通过源码、本地二进制或 Docker 运行。

## 运行

从 [Releases](https://github.com/fa1seut0pia/mirror-speed-test/releases) 下载后运行

默认监听 `http://127.0.0.1:58080`。
如果端口被占用，会自动递增尝试下一个可用端口。
当监听地址为 `127.0.0.1`/`localhost` 时，启动后会尝试自动打开默认浏览器。

Linux/macOS ：

```bash
chmod +x mirror-speed-test*
./mirror-speed-test-*

# 如需指定端口
MST_PORT=9000 ./mirror-speed-test-*
```

Windows （PowerShell）：

```powershell
.\mirror-speed-test-windows-x64.exe

# 如需指定端口
$env:MST_PORT = "9000"; .\mirror-speed-test-windows-x64.exe
```

## Docker

```bash
docker run --rm -p 58080:58080 ghcr.io/fa1seut0pia/mirror-speed-test:latest
```

构建并运行：

```bash
docker build -t mirror-speed-test:local .
docker run --rm -p 58080:58080 mirror-speed-test:local
```

## 可选环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MST_HOST` | `127.0.0.1` | 监听地址 |
| `MST_PORT` | `58080` | 监听端口 |
| `MST_OPEN_BROWSER` | `1` | 是否在本机监听时自动打开浏览器（`0/false` 关闭） |


## 开发

```bash
git clone https://github.com/fa1seut0pia/mirror-speed-test.git
cd mirror-speed-test
python3 app.py
```

## 开源协议

本项目使用 [MIT License](./LICENSE)。

## 注意事项

- 这是后端测速，不受浏览器 CORS 限制
- 速度结果反映样本文件下载表现，不完全等价于 `docker pull` / `npm install` 等工具的最终体验
- 某些镜像站可能不支持 `Range`；服务会尽量读取前 N MB 后提前结束
- 多次测速可能受到镜像站缓存、限流和线路波动影响

## 致谢

[Linux.do](https://linux.do)

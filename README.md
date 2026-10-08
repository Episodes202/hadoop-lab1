# Hadoop 实验一

本项目实现 Linux/Hadoop 常用操作、HDFS 的 Java 与 Shell 双实现，以及三个 MapReduce 程序。

## 环境

在 Ubuntu 24.04.3（WSL 2）、OpenJDK 17.0.20、Hadoop 3.5.0 上验证。Java API 从本机 Hadoop 安装获取，无需 Maven 下载依赖。先准备 HDFS 和 YARN，并保持 WSL 终端打开。

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/hadoop/hadoop
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export LANG=C.UTF-8 LC_ALL=C.UTF-8
# 仅在对应服务尚未运行时启动；不要重复格式化 HDFS。
start-dfs.sh
start-yarn.sh
jps
```

## 编译与 MapReduce

```bash
bash build.sh
bash scripts/run_mapreduce.sh
python3 tests/verify_mapreduce.py
```

`run_mapreduce.sh` 使用新的时间戳目录，运行3个课程样例和3个边界数据集。若本地 `results/*.txt` 已存在，请先将旧的 `results` 目录改名保留，再重跑。HDFS 路径记录在 `results/hdfs-base.txt`。

- `dedupe`：规范化行内空白后，以完整记录为 key，Combiner/Reducer 每个 key 输出一次。
- `sort`：使用 LongWritable 数值排序，单个 Reducer 维护全局位次；重复整数各占一个位次。
- `family`：以中间辈为连接键，组合孩子集合与父母集合；忽略表头，去除重复关系。单 Reducer 的 emitted 集合还能去除跨中间节点重复的祖孙对，适用于本次小规模数据；大规模应用应改为两阶段连接加去重，以避免集合占用过多内存。

## HDFS 双实现

Java 命令前缀：`hadoop jar build/experiment1.jar lab.HdfsTool`

Shell 命令前缀：`bash scripts/hdfs_ops.sh`

二者接受相同子命令：

| 子命令 | 参数 | 行为 |
|---|---|---|
| upload | LOCAL HDFS append或overwrite | 创建、追加或覆盖；省略模式时提示输入 |
| download | HDFS LOCAL_DIR | 同名时自动添加序号，不覆盖已有文件 |
| cat | HDFS_FILE | 原样输出文件内容 |
| stat | HDFS_PATH | 权限、大小、修改时间、路径 |
| list | HDFS_DIR | 递归列出文件元数据 |
| touch | HDFS_FILE | 自动创建父目录，再创建空文件；已存在则拒绝 |
| mkdir | HDFS_DIR | 递归创建目录 |
| rmdir | HDFS_DIR | 只删除空目录，拒绝非空目录 |
| append | HDFS_FILE LOCAL_CONTENT head或tail | 在头部或尾部添加文件中的字节 |
| rm | HDFS_FILE | 删除指定文件 |
| mv | SRC DST | 移动文件，自动建父目录，拒绝已有目标 |

```bash
python3 tests/hdfs_integration.py
```

测试会使用专用 HDFS 路径，验证26个检查点。`FileStatus` 无通用创建时间字段，因此明确显示“creation=UNAVAILABLE”，不把修改时间冒充创建时间。Java 头部追加通过同目录临时文件与 HDFS 原子 rename 替换；Shell 版本下载拼接后重新上传，适用于单用户、无并发写入的实验。并发编辑、ACL/xattr保留和生产级故障恢复不在本实验范围内。

## Linux 练习

`scripts/linux_basics.sh` 覆盖目录、文件、时间、所有者、查找、归档和 HDFS 上传下载。脚本涉及课程要求的 `/usr`、`/test` 等路径，需要 root；对预先存在的练习路径直接拒绝操作。实际本次运行使用临时 mount namespace 和 copy-on-write 根目录隔离，未修改宿主现有同名练习文件。

`more -10 ~/.bashrc` 需在交互终端运行，按空格翻页、按 q 退出。日志不纳入公开代码仓库，因为查看 `.bashrc` 的输出属于用户本地环境。

## 结果

课程样例：合并去重11行得到9行；数值排序11行；祖孙关系14条边得到12对关系（另有表头）。额外测试包含空白行、重复行、负数、重复整数、64位边界、重复亲缘边及跨中间节点的重复关系。所有结果由独立 Python 参考算法逐项核对。

公开仓库仅应包含本目录中未被 `.gitignore` 排除的源码、脚本、样例数据和说明；报告、邮箱、个人 `.bashrc`、日志和本地配置均不在公开内容中。

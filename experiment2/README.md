# 实验二 文档倒排索引

本实验使用一个 Hadoop MapReduce 作业，为 10 篇公开英文技术文档建立“单词 → 文档词频与总词频”的倒排索引。语料来自 Apache Hadoop 3.4.2 官方文档；程序实际运行环境为 WSL2 Ubuntu、Hadoop 3.5.0、OpenJDK 17.0.20.1。保留了固定语料快照、128 个停词、独立校验程序和完整示例索引。

## 文件

- `src/lab/InvertedIndex.java`：Mapper、Combiner、Reducer 和提交入口。
- `data/corpus/document01.txt` 至 `document10.txt`：UTF-8 英文文档。
- `data/sources.json`：每篇文档的标题、原网页、下载时间、字节数及 SHA-256。
- `data/stopwords.txt`：完整停词表，每行一个词。
- `data/edge/`：4 个边界文档，包括空文件和仅含停词的文件。
- `scripts/download_corpus.py`：从公开网页重新采集语料，Python 标准库实现。
- `scripts/run_lab.sh`：编译、上传 HDFS、运行 3 个作业并校验结果。
- `tests/verify_index.py`：独立的 Python Counter 参考算法，核对全部倒排项。
- `tests/test_verifier.py`：验证校验器能拒绝错误输出的 14 项测试。
- `examples/inverted-index.txt`：实际主作业的全部 2,890 行结果。
- `examples/verification-summary.json`：去除本地路径后的统计及核验结果。

## 编译和运行

进入 WSL2 的 Ubuntu，确认 HDFS 与 YARN 已启动，并且 `yarn node -list` 显示一个 RUNNING 节点。环境变量可按本机安装位置调整：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/hadoop/hadoop
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
export LANG=C.UTF-8 LC_ALL=C.UTF-8
jps -l
yarn node -list
bash build.sh
bash scripts/run_lab.sh
python3 tests/test_verifier.py
```

需要启动服务时，先检查进程，仅启动尚未运行的 NameNode、DataNode、SecondaryNameNode、ResourceManager 和 NodeManager。不要重新格式化已有 NameNode；保持 WSL 会话打开即可保留服务。本实验全部运行在已有 Hadoop 伪分布式环境。

运行脚本使用 `/user/当前用户/experiment2/时间戳-进程号` 作为新的 HDFS 目录，不覆盖原实验数据。结果写入新的 `results/时间戳-进程号/`，路径记录在 `results/latest-run.txt`。主作业使用 1 个 Reducer；两个边界作业均使用 2 个 Reducer，分别开启和关闭 Combiner。

直接提交程序：

```bash
hadoop jar build/experiment2.jar lab.InvertedIndex INPUT OUTPUT STOPWORDS [REDUCERS]
# 示例路径需要替换为实际已上传的 HDFS 文件/目录；OUTPUT 必须尚不存在。
```

`STOPWORDS` 是 HDFS 或其他 Hadoop 文件系统中的停词文件，Driver 用 `addCacheFile` 将它分发到任务工作目录。默认开启 Combiner；在三个路径参数之前添加 `-Dlab.index.combiner.enabled=false` 可关闭。Reducer 个数必须为正。

重新采集到新的目录而不覆盖已验证语料：

```bash
python3 scripts/download_corpus.py --output local/new-download
```

网页可能改变，要复现本次计数，应使用仓库自带的 `data/corpus` 快照。

## 算法和统计口径

1. 从文本逐行提取 `[A-Za-z]+`，再使用与语言区域无关的小写转换。
2. 过滤停词，Mapper 发出 `word → (document, 1)`。
3. Combiner 对同一词、同一文档的计数求和，保留同样的中间类型。Combiner 不执行、执行一次或多次都不改变数学结果。
4. Shuffle 将同一词送入同一 Reducer。Reducer 对每篇文档计数求和，按文档名排序，追加语料总频次。

输出使用一个 TAB 分隔单词与列表，分号分隔倒排项，最后以句点结束，例如实际输出：

```text
index	<document03.txt,1>;<total,1>.
search	<document04.txt,1>;<document09.txt,2>;<total,3>.
mapreduce	<document01.txt,14>;<document02.txt,25>;<document04.txt,1>;<document05.txt,1>;<document09.txt,110>;<document10.txt,2>;<total,153>.
```

这里的 `total` 是该词在全部文档中的出现次数，不是文档数；没有出现该词的文档不输出 0 频次。单 Reducer 的主输出按单词排序；多个 Reducer 的文件各自有序，不能宣称将分区简单拼接后仍全局有序。

## 实测结果

| 数据集 | 文档 | 原始词 | 过滤停词 | 有效词 | 唯一词 | 文档倒排项 |
|---|---:|---:|---:|---:|---:|---:|
| 正式语料 | 10 | 33,481 | 11,901 | 21,580 | 2,890 | 5,740 |
| 边界语料 | 4 | 57 | 35 | 22 | 11 | 14 |

三次作业均在真实 YARN 中完成，最终状态 SUCCEEDED。独立算法核对全部词、全部文档计数及总数。两个边界作业的分区输出逐字节一致。校验器的 14 项测试通过，包括总数错误、漏词、重复词、停词泄漏以及总数相同但分文档计数错误等负例。

主作业 Combiner 将 21,580 个 Mapper 输出记录合并为 5,740 个记录；这是本语料下的实际计数，不能据此推断普遍性能提升。

```bash
python3 tests/verify_index.py data/corpus data/stopwords.txt RESULTS_DIR \
  --min-documents 10 --summary verification.json
```

## 范围和限制

分词针对 ASCII 英文，不做词干还原、短语索引或词位索引。标点和数字作为分隔符；例如 `map-reduce` 分成两个词，重音字符可能把词拆开，边界语料明确测试该行为。正文中保留了文档自带的目录、命令及代码片段，因此统计是技术文档正文的词频，不等同于自然语言散文词频。

文档 ID 使用文件 basename，Driver 拒绝重复 basename、保留名 `total` 以及含空白或输出分隔符的文件名。Reducer 为单个词保存文档频次表，适用于本实验；超大规模、高文档频率词需要流式归并或分阶段处理。

公开目录不包含实验报告、个人资料或本地运行日志。语料来源和变更说明见 `NOTICE.txt`，原始 Apache 许可见 `LICENSE-APACHE-2.0.txt`。

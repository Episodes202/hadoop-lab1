#!/usr/bin/env bash
# Run as root in the lab WSL distribution. Never overwrite pre-existing paths.
set -euo pipefail
LAB_USER="${LAB_USER:-hadoop}"
LAB_HOME="$(getent passwd "$LAB_USER" | cut -d: -f6)"
for p in /tmp/a /tmp/a1 /tmp/test /tmp/hello /usr/bashrc1 /usr/test /usr/test2 /test /test.tar.gz /usr/local/hadoop; do
  if [ -e "$p" ] || [ -L "$p" ]; then echo "Refusing to alter existing path: $p" >&2; exit 1; fi
done
run() { printf '\n$'; printf ' %q' "$@"; printf '\n'; "$@"; }
student() { run runuser -u "$LAB_USER" -- "$@"; }
student bash -c 'cd /usr/local; pwd; cd ..; pwd; cd ~; pwd'
student ls -la /usr
student bash -c 'cd /tmp; mkdir a; ls -ld /tmp/*/; mkdir -p a1/a2/a3/a4; find a1 -type d; rmdir a; rmdir -p a1/a2/a3/a4; ls -ld /tmp/*/'
run cp "$LAB_HOME/.bashrc" /usr/bashrc1
student mkdir /tmp/test
run cp -r /tmp/test /usr/test
run mv /usr/bashrc1 /usr/test/
run mv /usr/test /usr/test2
run ls -la /usr/test2
run rm /usr/test2/bashrc1
run rmdir /usr/test2
student cat "$LAB_HOME/.bashrc"
student tac "$LAB_HOME/.bashrc"
student head -n 20 "$LAB_HOME/.bashrc"
student head -n -50 "$LAB_HOME/.bashrc"
student tail -n 20 "$LAB_HOME/.bashrc"
student tail -n +51 "$LAB_HOME/.bashrc"
student touch /tmp/hello
run stat /tmp/hello
student touch -d '5 days ago' /tmp/hello
run stat /tmp/hello
run chown root /tmp/hello
run ls -l /tmp/hello
student find "$LAB_HOME" -name .bashrc -type f
run mkdir /test
run tar -czf /test.tar.gz -C / test
run tar -xzf /test.tar.gz -C /tmp
run tar -tzf /test.tar.gz
student grep -n examples "$LAB_HOME/.bashrc"
run mkdir /usr/local/hadoop
run chown "$LAB_USER:$LAB_USER" /usr/local/hadoop
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/test
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -ls /user/hadoop
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -put "$LAB_HOME/.bashrc" /user/hadoop/test/.bashrc-qin-20261008
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -ls /user/hadoop/test
student env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /home/hadoop/hadoop/bin/hdfs dfs -get /user/hadoop/test /usr/local/hadoop/
run sha256sum "$LAB_HOME/.bashrc" /usr/local/hadoop/test/.bashrc-qin-20261008
cmp "$LAB_HOME/.bashrc" /usr/local/hadoop/test/.bashrc-qin-20261008
echo 'LINUX_AND_HADOOP_BASICS_PASS (more paging is tested separately)'

#!/usr/bin/env python3
"""Run both implementations against real HDFS, asserting content and refusal cases."""
import json
import os
from pathlib import Path
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.environ.update(LANG='C.UTF-8',LC_ALL='C.UTF-8',JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64')
HADOOP=Path(os.environ.get('HADOOP_HOME','/home/hadoop/hadoop'))
OUT=ROOT/'results'
OUT.mkdir(exist_ok=True)
log=[]
checks=[]
test_id=str(time.time_ns())
def run(args: list[str], success: bool=True) -> str:
    p=subprocess.run(args,text=True,encoding='utf-8',stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    log.append('$ '+' '.join(args)+'\n'+p.stdout+p.stderr+f'\nEXIT={p.returncode}\n')
    (OUT/'hdfs-test.log').write_text('\n'.join(log),encoding='utf-8')
    if (p.returncode==0)!=success:
        raise AssertionError(log[-1])
    return p.stdout
def check(name: str, condition: bool=True) -> None:
    if not condition: raise AssertionError(name)
    checks.append(name)
    print('PASS',name,flush=True)
for backend in ['java','shell']:
    base=f'/user/hadoop/experiment1-qin-20261008/hdfs-{backend}-{test_id}'
    prefix=([str(HADOOP/'bin/hadoop'),'jar','build/experiment1.jar','lab.HdfsTool']
            if backend=='java' else ['bash','scripts/hdfs_ops.sh'])
    def op(*args: str, success: bool=True) -> str:
        return run(prefix+list(args),success)
    path=base+'/nested/base.txt'
    op('upload','data/hdfs/base.txt',path,'overwrite')
    check(backend+': new upload',op('cat',path)=='alpha\nbeta\n')
    op('upload','data/hdfs/extra.txt',path,'append')
    check(backend+': upload append',op('cat',path)=='alpha\nbeta\ngamma\n')
    op('append',path,'data/hdfs/prefix.txt','head')
    op('append',path,'data/hdfs/extra.txt','tail')
    check(backend+': head and tail',op('cat',path)=='START\nalpha\nbeta\ngamma\ngamma\n')
    op('upload','data/hdfs/extra.txt',path,'overwrite')
    check(backend+': overwrite',op('cat',path)=='gamma\n')
    local=OUT/backend
    local.mkdir(exist_ok=True)
    first=Path(op('download',path,str(local)).strip())
    second=Path(op('download',path,str(local)).strip())
    check(backend+': rename download collision',first!=second and first.read_bytes()==(ROOT/'data/hdfs/extra.txt').read_bytes() and first.read_bytes()==second.read_bytes())
    status=op('stat',path)
    check(backend+': metadata', '6' in status and ('UNAVAILABLE' in status or 'unavailable' in status))
    op('touch',base+'/deep/a/b/empty.txt')
    listing=op('list',base)
    check(backend+': recursive metadata', 'empty.txt' in listing and 'base.txt' in listing)
    op('touch',base+'/deep/a/b/empty.txt',success=False)
    check(backend+': refuse existing create')
    op('mkdir',base+'/empty/child')
    op('rmdir',base+'/empty/child')
    op('rmdir',base+'/empty')
    check(backend+': mkdir parents and remove empty')
    op('rmdir',base+'/nested',success=False)
    check(backend+': refuse nonempty directory',op('cat',path)=='gamma\n')
    op('mv',path,base+'/moved/renamed.txt')
    check(backend+': move',op('cat',base+'/moved/renamed.txt')=='gamma\n')
    op('cat',path,success=False)
    op('rm',base+'/moved/renamed.txt')
    op('cat',base+'/moved/renamed.txt',success=False)
    check(backend+': delete file')
    unicode_local=local/'unicode.txt'
    unicode_local.write_text('中文数据\n',encoding='utf-8')
    op('upload',str(unicode_local),base+'/unicode.txt','overwrite')
    check(backend+': UTF-8 roundtrip',op('cat',base+'/unicode.txt')=='中文数据\n')
(OUT/'hdfs-test.log').write_text('\n'.join(log),encoding='utf-8')
(OUT/'hdfs-verification.json').write_text(json.dumps({'passed':len(checks),'checks':checks},ensure_ascii=False,indent=2),encoding='utf-8')
print('ALL_HDFS_TESTS_PASS',len(checks))

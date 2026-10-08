#!/usr/bin/env python3
"""Independent reference algorithms; compare real MapReduce output with expected data."""
from pathlib import Path
import json
import re
ROOT=Path(__file__).resolve().parents[1]
summary=[]
for dataset in ['dedupe','sort','family','edge-dedupe','edge-sort','edge-family']:
    job=dataset.removeprefix('edge-')
    lines=[line.strip() for f in sorted((ROOT/'data'/dataset).glob('*.txt')) for line in f.read_text(encoding='utf-8').splitlines() if line.strip()]
    actual=(ROOT/'results'/f'{dataset}.txt').read_text(encoding='utf-8').splitlines()
    if job=='dedupe':
        expected=sorted({'\t'.join(line.split()) for line in lines})
        assert actual==expected,(dataset,actual,expected)
    elif job=='sort':
        expected=[f'{i}\t{n}' for i,n in enumerate(sorted(map(int,lines)),1)]
        assert actual==expected,(dataset,actual,expected)
    else:
        edges={tuple(line.split()) for line in lines if line.lower()!='child parent'}
        expected={(child,grandparent) for child,parent in edges for middle,grandparent in edges if parent==middle}
        assert actual[0]=='grandchild\tgrandparent'
        assert {tuple(line.split('\t')) for line in actual[1:]}==expected
        assert len(actual)-1==len(expected)
    log=(ROOT/'results'/f'{dataset}.log').read_text(encoding='utf-8')
    app=re.search(r'Submitted application (application_\d+_\d+)',log).group(1)
    assert 'completed successfully' in log
    item={'dataset':dataset,'input_records':len(lines),'output_records':len(actual)-(job=='family'),'application':app,'verified':True}
    summary.append(item)
    print('PASS',dataset,item['output_records'],app)
(ROOT/'results'/'mapreduce-verification.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')

"""Bounded newc image editing without extracting archive paths or symlinks."""
from dataclasses import dataclass
import gzip,hashlib,stat
MAX_IMAGE=256<<20
MAX_MEMBERS=10000
REPLACEMENTS={'bin/run_app','bin/uflow_env_payload','bin/uflow_pool_agent','bin/linqu_mem_service_provider_obmm'}
def check(ok,why):
    if not ok:raise ValueError(why)
@dataclass(frozen=True)
class Entry:
    name:str
    fields:tuple
    data:bytes
    record:bytes
def parse(raw):
    check(type(raw) is bytes and 0<len(raw)<=MAX_IMAGE,'bounded newc archive required')
    entries=[];seen=set();pos=0
    while True:
        check(pos+110<=len(raw) and raw[pos:pos+6]==b'070701','exact newc header required')
        start=pos;fields=tuple(int(raw[pos+6+8*i:pos+14+8*i],16) for i in range(13));size=fields[6];namesize=fields[11]
        check(1<=namesize<=4096 and fields[12]==0,'bounded canonical name/no CRC data required')
        pos+=110;check(pos+namesize<=len(raw),'truncated newc name');namebytes=raw[pos:pos+namesize];check(namebytes[-1:]==b'\0' and b'\0' not in namebytes[:-1],'exact newc name')
        name=namebytes[:-1].decode('utf-8');pos=(pos+namesize+3)&~3;check(size<=MAX_IMAGE and pos+size<=len(raw),'truncated newc body');data=raw[pos:pos+size];pos=(pos+size+3)&~3
        check(pos<=len(raw),'truncated newc padding')
        if name=='TRAILER!!!':
            check(size==0 and not any(raw[pos:]),'single newc archive/trailing padding required');break
        clean=name[2:] if name.startswith('./') else name
        check(clean=='.' or (clean and not clean.startswith('/') and all(p not in ('','..','.') for p in clean.split('/'))),'relative bounded archive path required')
        check(clean not in seen and len(entries)<MAX_MEMBERS,'duplicate/excess newc member');seen.add(clean);entries.append(Entry(clean,fields,data,raw[start:pos]))
    check(entries,'nonempty base image required');return entries
def record(name,data,ino):
    check(0<=ino<1<<32,'newc inode bound')
    encoded=name.encode()+b'\0';fields=(ino,0 if name=='TRAILER!!!' else stat.S_IFREG|0o755,0,0,1,0,len(data),0,0,0,0,len(encoded),0)
    raw=b'070701'+b''.join(f'{n:08x}'.encode() for n in fields)+encoded;raw+=b'\0'*(-len(raw)%4);raw+=data;return raw+b'\0'*(-len(data)%4)
def overlay(base,replacements):
    check(type(replacements) is dict and set(replacements)==REPLACEMENTS and all(type(v) is bytes and v for v in replacements.values()),'exact four production replacements required')
    entries=parse(base);ino=max(e.fields[0] for e in entries)+1;result=[];remaining=dict(replacements)
    for e in entries:
        if e.name in remaining:
            check(stat.S_ISREG(e.fields[1]) and e.fields[4]==1,'replacement may not follow a symlink/hardlink')
            result.append(record(e.name,remaining.pop(e.name),ino));ino+=1
        else:result.append(e.record)
    for name,data in sorted(remaining.items()):result.append(record(name,data,ino));ino+=1
    # The trailer has no file semantics; write its standard empty record.
    result.append(record('TRAILER!!!',b'',ino));raw=b''.join(result);raw+=b'\0'*(-len(raw)%512);after=parse(raw)
    original={e.name:e for e in entries};final={e.name:e for e in after}
    check(set(final)==set(original)|REPLACEMENTS,'image member set drift')
    for name,e in original.items():
        if name not in REPLACEMENTS:check(final[name].record==e.record,'unrelated image member changed')
    for name,data in replacements.items():check(final[name].data==data and final[name].fields[1]==stat.S_IFREG|0o755,'production image replacement mismatch')
    return raw
def inventory(raw):
    return {e.name:{'mode':oct(e.fields[1]),'uid':e.fields[2],'gid':e.fields[3],'nlink':e.fields[4],'bytes':len(e.data),'sha256':hashlib.sha256(e.data).hexdigest()} for e in parse(raw)}
def read_gzip(path):
    with gzip.open(path,'rb') as f:raw=f.read(MAX_IMAGE+1)
    check(len(raw)<=MAX_IMAGE,'uncompressed image bound');return raw

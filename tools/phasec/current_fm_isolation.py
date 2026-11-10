"""Metadata-only fresh namespace isolation from the full retained historical62.

No holder/normal-release inference or payload reads. Final exact union still
requires the independent new-profile E registration/audit producer.
"""
import hashlib,json,os,stat
from pathlib import Path
from accepted_producers import AP_ROOT,ap_require

BASE_SHA='a8c86627872d4883f5d76c90ec6c31f268e3cc48d3f638b72d996576e32cdcd3'

def _files(root):
    todo=[Path(root)];found=set();count=0
    while todo:
        directory=todo.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                count+=1;ap_require(count<=512,'bounded exact FM namespace required')
                p=Path(entry.path);m=entry.stat(follow_symlinks=False)
                ap_require(not stat.S_ISLNK(m.st_mode),'FM namespace alias')
                if stat.S_ISDIR(m.st_mode):todo.append(p)
                else:found.add(p)
    return found

def historical62():
    path=Path(__file__).resolve().parents[2]/'examples/mem_service_pool/current_terminal_production/historical62-identities.json'
    raw=path.read_bytes();ap_require(len(raw)==35280 and hashlib.sha256(raw).hexdigest()==BASE_SHA,'fixed historical62 identity source drift')
    value=json.loads(raw);rows=value['files'];ap_require(len(rows)==62 and len({r['path'] for r in rows})==62,'exact historical62 source rows')
    aliases=set();roots=set()
    for row in rows:
        p=Path(row['path']);ap_require(p.is_relative_to(AP_ROOT/'runs') and p.resolve()==p,'canonical historical FM path')
        root=next((q for q in p.parents if q.name=='fm' and q.parent.parent==AP_ROOT/'runs'),None)
        ap_require(root is not None,'historical FM root');roots.add(root)
        m=p.lstat();ap_require((m.st_dev,m.st_ino,m.st_uid,stat.S_IFMT(m.st_mode),m.st_size)==(row['dev'],row['inode'],row['uid'],row['file_type'],row['bytes']),'historical carrier/marker replaced')
        ap_require(m.st_uid==os.geteuid() and m.st_nlink==1,'owned single-link historical carrier required')
        a=set(map(tuple,row['kernel_inode_aliases']));ap_require((m.st_dev,m.st_ino) in a,'historical kernel identity alias absent');aliases.update(a)
    actual=set().union(*(_files(root) for root in roots));ap_require(actual=={Path(r['path']) for r in rows},'unexpected or missing historical FM file')
    return aliases,{'historical_files':62,'historical_identity_source_sha256':BASE_SHA,'quarantined_marker':value['quarantined_current_marker'],
        'old_marker_registered_as_normal':False,'FM_payload_contents_read':False,'SDK_ACK_inferred':False}

def _validate_new_rows(out,rows,old_aliases):
    ap_require(len(rows)==10 and len({r['path'] for r in rows})==10,'exact ten pre-business new carriers required')
    seen=set()
    for row in rows:
        p=Path(row['path']);ap_require(p.is_relative_to(out/'fm') and p.resolve()==p,'canonical new FM path')
        m=p.lstat();kind=stat.S_IFMT(m.st_mode)
        ap_require(kind in (stat.S_IFREG,stat.S_IFIFO,stat.S_IFSOCK) and (m.st_dev,m.st_ino,m.st_uid,kind)==(row['dev'],row['inode'],row['uid'],row['file_type']) and m.st_uid==os.geteuid() and m.st_nlink==1,'actual owned new carrier identity mismatch')
        aliases=set(map(tuple,row['kernel_inode_aliases']));ap_require((m.st_dev,m.st_ino) in aliases and not aliases.intersection(old_aliases|seen),'new carrier aliases overlap retained62 or another carrier')
        seen.update(aliases)
    ap_require(_files(out/'fm')=={Path(r['path']) for r in rows},'unknown extra new FM file before canary')
    return seen

def verify_current_fm(out):
    out=Path(out);ap_require(out.parent==AP_ROOT/'runs' and out.resolve()==out and out.name not in ('c013','c027','c033','c039','c044','c064'),'exact new current-case FM namespace')
    old,report=historical62();raw=(out/'fm-carrier-active.json').read_bytes();ap_require(len(raw)<=65536,'bounded new active inventory')
    rows=json.loads(raw)['files'];_validate_new_rows(out,rows,old)
    return {**report,'new_files':10,'independent_from_all_historical62':True,'new_inventory_sha256':hashlib.sha256(raw).hexdigest()}

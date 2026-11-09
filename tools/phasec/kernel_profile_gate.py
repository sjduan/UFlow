"""Host-only check of the actual Image, its embedded config and build provenance.

Necessary capability evidence, never a Guest PASS or authority to boot. The
selected manifest digest must come from the reviewed immutable launch config.
"""
import hashlib,json,os,re,stat,zlib
from pathlib import Path
KERNEL='c2def7c931e6451d89727dec0c6f892abe28ba00'
BASE='18f81737d7e2a7bdaa0b72a3c86bf5b2c7ff3fd742434437ffa8926be5deb4fd'
BASE_CONFIG='f0400a59db1d9eddecdc57a828b887075aa611a07f19e369371029af755152d8'
REQUIRED=('CONFIG_UB_UMMU_CORE','CONFIG_UB_UMMU_CORE_DRIVER','CONFIG_UB_UMMU_SVA_SEPARATED_PAGES','CONFIG_UB_UBUS','CONFIG_IOMMU_API','CONFIG_IKCONFIG')
MAX_IMAGE,MAX_CONFIG=128<<20,1<<20
def require(ok,message):
    if not ok:raise ValueError('kernel capability gate: '+message)
def sha(b):return hashlib.sha256(b).hexdigest()
def unique(items):
    d={}
    for k,v in items:
        require(k not in d,'duplicate manifest key');d[k]=v
    return d
def bounded(p,maximum):
    require(not p.is_symlink() and stat.S_ISREG(p.stat().st_mode) and p.stat().st_size<=maximum,'bounded regular file')
    fd=os.open(p,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_CLOEXEC',0))
    with os.fdopen(fd,'rb') as f:
        before=os.fstat(f.fileno());require(stat.S_ISREG(before.st_mode) and before.st_size<=maximum,'actual bounded regular FD')
        b=f.read(maximum+1);after=os.fstat(f.fileno())
    require(len(b)==before.st_size==after.st_size and len(b)<=maximum and before.st_mtime_ns==after.st_mtime_ns,'file changed while hashing');return b
def embedded(image):
    require(len(image)<=MAX_IMAGE and image.count(b'IKCFG_ST')==image.count(b'IKCFG_ED')==1,'one complete embedded config')
    start=image.index(b'IKCFG_ST')+8;end=image.index(b'IKCFG_ED');require(0<end-start<=MAX_CONFIG,'bounded ordered config stream')
    z=zlib.decompressobj(31)
    try:config=z.decompress(image[start:end],MAX_CONFIG+1)
    except zlib.error as e:raise ValueError('kernel capability gate: invalid compressed config') from e
    require(z.eof and not z.unused_data and not z.unconsumed_tail and 0<len(config)<=MAX_CONFIG,'complete single bounded gzip config')
    return config
def values(raw):
    require(len(raw)<=MAX_CONFIG and b'\0' not in raw,'bounded config text')
    result={}
    for line in raw.decode('utf-8').splitlines():
        m=re.fullmatch(r'(CONFIG_[A-Z0-9_]+)=(.*)',line)
        disabled=re.fullmatch(r'# (CONFIG_[A-Z0-9_]+) is not set',line)
        if m:key,value=m.groups()
        elif disabled:key,value=disabled[1],'n'
        else:continue
        require(key not in result,'duplicate config option');result[key]=value
    require(result,'nonempty config');return result
def check(image_path,manifest_path,manifest_sha256,base_image,*,allowed_root):
    paths=[image_path,manifest_path,base_image]
    for p in paths:
        require(p.is_absolute() and p==p.resolve() and p.is_relative_to(allowed_root.resolve()),'task-local canonical absolute path')
        require(not any(q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(allowed_root)),'no symlink path components')
    raw=bounded(manifest_path,1<<20);require(sha(raw)==manifest_sha256,'reviewed producer manifest digest')
    m=json.loads(raw,object_pairs_hook=unique)
    required=['schema','interface','profile_id','kernel_commit','base_Image_sha256','base_embedded_config_sha256','resolved_config_diff','source_manifest_sha256','compiler_and_build_argv','outputs','embedded_config_sha256','required_builtin_config','real_symbol_and_obmm_call_evidence','old_baseline_unchanged','runtime_tested']
    require(all(k in m for k in required),'complete producer provenance')
    require(type(m['schema']) is int and m['schema']==1 and m['interface']=='mem-service-private-kernel-profile-v1' and m['profile_id']=='mem-service-gsva-separated-v1' and
        m['kernel_commit']==KERNEL and m['base_Image_sha256']==BASE and m['base_embedded_config_sha256']==BASE_CONFIG,'versioned exact profile/source/base identity')
    require(type(m['required_builtin_config']) is list and len(m['required_builtin_config'])==len(REQUIRED) and set(m['required_builtin_config'])==set(REQUIRED) and m['runtime_tested'] is False and m['old_baseline_unchanged'] is True,'host-only complete profile')
    require(isinstance(m['compiler_and_build_argv'],list) and m['compiler_and_build_argv'] and m['real_symbol_and_obmm_call_evidence'],'actual compiler/link/OBMM provenance')
    outputs=m['outputs'];require(isinstance(outputs,dict),'named producer outputs')
    data={}
    for name,limit in [('Image',MAX_IMAGE),('vmlinux',512<<20),('System.map',64<<20),('.config',MAX_CONFIG),('source-manifest.json',4<<20)]:
        row=outputs.get(name);require(isinstance(row,dict) and {'path','bytes','sha256'}<=row.keys(),'bound '+name)
        p=Path(row['path']);require(type(row['bytes']) is int and row['bytes']>0 and p.is_absolute() and p==p.resolve() and p.is_relative_to(allowed_root.resolve()) and not any(q.is_symlink() for q in [p,*p.parents] if q.is_relative_to(allowed_root)),'controlled output '+name)
        b=bounded(p,limit);require(len(b)==row['bytes'] and sha(b)==row['sha256'],'actual output hash/size '+name);data[name]=b
    require(Path(outputs['Image']['path'])==image_path and sha(data['Image'])!=BASE,'selected new Image membership; old disabled Image denied')
    require(data['vmlinux'][:4]==b'\x7fELF' and len(data['vmlinux'])>20 and int.from_bytes(data['vmlinux'][18:20],'little')==183,'actual aarch64 vmlinux')
    require(len(re.findall(rb'(?m)^[0-9a-fA-F]+ [Tt] ummu_sva_matt_map$',data['System.map']))==1,'actual linked separated-pages symbol')
    config=embedded(data['Image']);v=values(config)
    require(config==data['.config'] and sha(config)==m['embedded_config_sha256'],'actual embedded/resolved config binding')
    require(all(v.get(k)=='y' for k in REQUIRED),'required options must be builtin y')
    baseline=bounded(base_image,MAX_IMAGE);require(sha(baseline)==BASE,'old Image remains unchanged')
    original=embedded(baseline);require(sha(original)==BASE_CONFIG,'old embedded config binding');before=values(original)
    diff={k:{'before':before.get(k),'after':v.get(k)} for k in before.keys()|v.keys() if before.get(k)!=v.get(k)}
    intended={'CONFIG_UB_UMMU_SVA_SEPARATED_PAGES':{'before':'n','after':'y'}}
    require(diff==intended and m['resolved_config_diff']==intended,'only reviewed minimal resolved delta')
    require(sha(data['source-manifest.json'])==m['source_manifest_sha256'],'actual source manifest binding')
    source=json.loads(data['source-manifest.json'],object_pairs_hook=unique)
    require(source.get('kernel_commit')==KERNEL and source.get('files'),'fixed source manifest identity/input closure')
    return {'profile_id':m['profile_id'],'manifest_sha256':manifest_sha256,'Image_sha256':sha(data['Image']),
        'embedded_config_sha256':sha(config),'required_builtin_config':list(REQUIRED),'runtime_tested':False}

def launch_gate(root,config):
    selected=config.get('kernel_profile');require(isinstance(selected,dict) and set(selected)=={'manifest','manifest_sha256'},'reviewed additional profile required')
    require(re.fullmatch('[0-9a-f]{64}',selected['manifest_sha256']) is not None,'manifest digest syntax')
    return check(Path(config['kernel']),Path(selected['manifest']),selected['manifest_sha256'],
        root/'src/ub_sim/guest-linux/aarch64/out/Image',allowed_root=root)

"""Independent corrected-QEMU profile consumer. New production pins pending.

Selection is case data; it cannot supply a validator trust anchor. The old
fixed executable remains its own explicit branch, never a requested-profile
fallback. Private HOST seams cannot mint production capture identities.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import types

ROOT=Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
LEGACY_BINARY=ROOT/'build/qemu/qemu-system-aarch64'
LEGACY_SHA='56fab4e5d8c50c559f76c8ddbae9d6e1661e4764a6f927db2c3706e8199e5d4c'
BASE_COMMIT='f2ef54f454b661e551a8619f9feb7406752a1b1a'
INTERFACE='mem-service-private-qemu-profile-v1'
PROFILE='mem-service-gsva-scatter-home-v1'
_ACCEPTED_PROFILE=None  # Requires manager acceptance of actual E raw pins.
_PRODUCTION=object()
_HOST=object()


def require(ok,detail):
    if not ok:raise ValueError('QEMU profile gate: '+detail)


@dataclass(frozen=True)
class _Identity:
    binary:Path
    sha256:str
    _serialized_provenance:str
    _seal:object

    def metadata(self):return json.loads(self._serialized_provenance)


def _identity(binary,pin,provenance,seal):
    return _Identity(Path(binary),pin,json.dumps(provenance,sort_keys=True),seal)


def _read(path,root,maximum):
    path=Path(path);root=Path(root)
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(root.resolve()),'canonical task-local member required')
    require(not any(p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(root)),'member path alias')
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    with os.fdopen(fd,'rb') as f:
        first=os.fstat(f.fileno());require(stat.S_ISREG(first.st_mode) and 0<first.st_size<=maximum and first.st_nlink==1,'bounded ordinary single-link member required')
        raw=f.read(maximum+1);last=os.fstat(f.fileno());named=path.lstat()
    attrs=lambda m:(m.st_dev,m.st_ino,m.st_size,m.st_mtime_ns,m.st_ctime_ns,m.st_nlink)
    require(attrs(first)==attrs(last)==attrs(named) and len(raw)==first.st_size,'profile member changed during observation')
    return raw


def _bound(ref,root,maximum):
    require(type(ref) is dict and set(ref)=={'path','bytes','sha256'},'exact independently accepted reference required')
    require(type(ref['bytes']) is int and type(ref['sha256']) is str and re.fullmatch('[0-9a-f]{64}',ref['sha256']),'reference types')
    raw=_read(ref['path'],root,maximum)
    require(len(raw)==ref['bytes'] and hashlib.sha256(raw).hexdigest()==ref['sha256'],'accepted source/member digest mismatch')
    return raw


def _selection(anchor):
    return {'profile_id':PROFILE,'manifest':anchor['manifest'],'validator':anchor['validator']}


def _validate(root,config,anchor,seal):
    # Every code/dependency/member byte is bound before the FIRST source exec.
    require(type(anchor) is dict and config.get('qemu_profile')==_selection(anchor),'exact accepted E profile selection required')
    require(config.get('qemu_binary')==anchor['binary']['path'],'selected QEMU member mismatch')
    manifest=_bound(anchor['manifest'],root,8<<20)
    binary=_bound(anchor['binary'],root,256<<20)
    require(binary[:4]==b'\x7fELF','actual ordinary QEMU ELF required')
    members=anchor['members'];require(set(members)=={'patch','source_closure','object_closure','link_closure'},'full source/patch/object/link reference closure required')
    for ref in members.values():_bound(ref,root,8<<20)
    dependencies=anchor['dependencies'];require(type(dependencies) is dict and len(dependencies)<=16,'bounded accepted dependency closure')
    sources={}
    for name,ref in [*dependencies.items(),('uflow_reviewed_qemu_profile',anchor['validator'])]:
        require(type(name) is str and re.fullmatch(r'uflow_[A-Za-z0-9_]+',name) and name not in sources,'unique private reviewed module name')
        sources[name]=(ref,_bound(ref,root,256<<10))
    modules={name:types.ModuleType(name) for name in sources};previous={name:sys.modules.get(name) for name in modules}
    try:
        for name,module in modules.items():
            module.__file__=sources[name][0]['path'];sys.modules[name]=module
        for name,(ref,raw) in sources.items():exec(compile(raw,ref['path'],'exec'),modules[name].__dict__)
        validator=modules['uflow_reviewed_qemu_profile'];require(callable(getattr(validator,'validate',None)),'accepted validate API missing')
        result=validator.validate(Path(anchor['manifest']['path']),anchor['manifest']['sha256'],Path(anchor['binary']['path']))
    finally:
        for name,old in previous.items():
            if old is None:sys.modules.pop(name,None)
            else:sys.modules[name]=old
    require(type(result) is dict and result.get('schema')==1 and result.get('interface')==INTERFACE and result.get('profile_id')==PROFILE and
            result.get('status')=='PASS_HOST_QEMU_PROFILE_ONLY' and result.get('producer_manifest_sha256')==anchor['manifest']['sha256'] and
            result.get('base_qemu_commit')==BASE_COMMIT and result.get('base_binary_sha256')==LEGACY_SHA and
            result.get('patch_sha256')==members['patch']['sha256'] and result.get('qemu_binary')==anchor['binary']['path'] and
            result.get('qemu_binary_sha256')==anchor['binary']['sha256'] and result.get('runtime_tested') is False and
            result.get('authority_to_boot_or_replace_existing_binary') is False,'actual producer result/identity/authority mismatch')
    for key in ('source_closure','object_closure','link_closure'):
        require(result.get(key)==members[key],'actual producer reference closure mismatch')
    # Detect changes caused during producer execution too; never return stale
    # identity merely because a validator returned an old cached JSON result.
    for ref in [anchor['manifest'],anchor['binary'],*members.values()]:_bound(ref,root,256<<20 if ref==anchor['binary'] else 8<<20)
    for ref,_ in sources.values():_bound(ref,root,256<<10)
    return _identity(anchor['binary']['path'],anchor['binary']['sha256'],{
        **result,'external_validator':anchor['validator'],'accepted_dependencies':dependencies,
        'profile_manifest':anchor['manifest'],'HOST_fixture_only':seal is _HOST,
        'boot_authority':False},seal)


def launch_qemu_gate(root,config):
    require(Path(root)==ROOT,'fixed task root required')
    if 'qemu_profile' in config:
        require(_ACCEPTED_PROFILE is not None,'not-ready: actual manager-accepted E QEMU manifest/validator/dependency/binary pins missing; no fallback or source execution')
        return _validate(root,config,_ACCEPTED_PROFILE,_PRODUCTION)
    require(config.get('qemu_binary')==str(LEGACY_BINARY),'legacy branch requires original exact fixed executable')
    raw=_read(LEGACY_BINARY,root,256<<20);require(hashlib.sha256(raw).hexdigest()==LEGACY_SHA,'original QEMU executable drift')
    return _identity(LEGACY_BINARY,LEGACY_SHA,{'branch':'original-fixed-QEMU','qemu_binary':str(LEGACY_BINARY),
        'qemu_binary_sha256':LEGACY_SHA,'base_qemu_commit':BASE_COMMIT,'HOST_fixture_only':False,'boot_authority':False},_PRODUCTION)


def capture_identity(config,identity):
    _require_production_identity(identity)
    require(config.get('qemu_binary')==str(identity.binary),'capture binary differs from pre-launch validated identity')
    if 'qemu_profile' in config:
        require(_ACCEPTED_PROFILE is not None and config['qemu_profile']==_selection(_ACCEPTED_PROFILE),'accepted new-profile binding required for capture')
        require(identity.metadata().get('profile_manifest')==_ACCEPTED_PROFILE['manifest'],'profile capture provenance mismatch')
    else:require(identity.binary==LEGACY_BINARY and identity.sha256==LEGACY_SHA,'original fixed capture branch required')
    return identity


def _require_production_identity(identity):
    require(isinstance(identity,_Identity) and identity._seal is _PRODUCTION,'actual production-validated QEMU identity required; caller hash/HOST fixture refused')
    return identity


def _host_validate_profile(root,config,anchor):
    """Explicit test-only mechanism; its identity cannot enter production capture."""
    return _validate(root,config,anchor,_HOST)


def _host_fixture_identity(binary,pin):
    return _identity(binary,pin,{'HOST_fixture_only':True,'boot_authority':False},_HOST)


def _require_host_identity(identity):
    require(isinstance(identity,_Identity) and identity._seal is _HOST,'explicit private HOST fixture required')
    return identity

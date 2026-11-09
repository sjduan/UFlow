"""Host launcher consumption of the independently reviewed E producer profile.

Execute only externally hash-pinned validator bytes, then consume its actual
producer/object closure. No ready flag, fallback, build or boot authority.
"""
import hashlib,os,stat,types
from pathlib import Path
INTERFACE='mem-service-private-kernel-profile-v1'
PROFILE='mem-service-gsva-separated-v1'
KERNEL='c2def7c931e6451d89727dec0c6f892abe28ba00'
BASE='18f81737d7e2a7bdaa0b72a3c86bf5b2c7ff3fd742434437ffa8926be5deb4fd'
MANIFEST_SHA='8cdb2550448363a127d6eb30b5cde5fa5c21a2f0427ce30a5b40532779d2b939'
VALIDATOR_SHA='a7ee8ce56544fd4391b6a35864e1a3a0a249005f271898b08940c5ee4a47c864'
IMAGE_SHA='95165fea380ed231b609d4a2a71cfe9b877cac52b7bc44e39a870c839a676ada'
CONFIG_SHA='789fb96c370aa7ad04bcf7aad2df9b97c7145132844459e88a10829fc46f3c78'
RELEASE=Path('implementation/kernel-profile-sep-v1/release-v2')
def require(ok,message):
    if not ok:raise ValueError('kernel capability gate: '+message)
def binding(root):
    release=root/RELEASE
    return {'manifest':str(release/'profile-manifest.json'),'manifest_sha256':MANIFEST_SHA,
            'validator':str(release/'uflow_kernel_profile.py'),'validator_sha256':VALIDATOR_SHA}
def canonical(path,root):
    require(path.is_absolute() and path==path.resolve() and path.is_relative_to(root.resolve()),'task-local canonical absolute path')
    require(not any(p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(root)),'no symlink path components')
def _load_validator(path):
    # Hash the actual ordinary file before compiling; execute those exact bytes
    # rather than importing/re-reading a potentially replaced path.
    m=path.lstat();require(stat.S_ISREG(m.st_mode) and 0<m.st_size<=32768,'bounded regular reviewed validator required')
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0)|getattr(os,'O_CLOEXEC',0))
    with os.fdopen(fd,'rb') as f:
        before=os.fstat(f.fileno());require(stat.S_ISREG(before.st_mode) and before.st_size<=32768,'actual regular validator FD')
        raw=f.read(32769);after=os.fstat(f.fileno())
    require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) and len(raw)==before.st_size,'validator changed while reading')
    require(hashlib.sha256(raw).hexdigest()==VALIDATOR_SHA,'external reviewed validator hash mismatch')
    module=types.ModuleType('reviewed_uflow_kernel_profile');module.__file__=str(path)
    exec(compile(raw,str(path),'exec'),module.__dict__)
    require(callable(getattr(module,'validate',None)),'reviewed validator API missing');return module
def _base_digest(root):
    path=root/'src/ub_sim/guest-linux/aarch64/out/Image';canonical(path,root)
    m=path.lstat();require(stat.S_ISREG(m.st_mode) and 0<m.st_size<=128<<20,'bounded regular old Image')
    digest=hashlib.sha256();fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    with os.fdopen(fd,'rb') as f:
        before=os.fstat(f.fileno());require(stat.S_ISREG(before.st_mode) and before.st_size==m.st_size,'actual old Image FD')
        count=0
        for _ in range(129):
            block=f.read(1<<20)
            if not block:break
            count+=len(block);digest.update(block)
        else:raise ValueError('old Image read bound')
        after=os.fstat(f.fileno())
    require(count==before.st_size==after.st_size and before.st_mtime_ns==after.st_mtime_ns,'old Image changed during check');return digest.hexdigest()
def launch_gate(root,config):
    expected=binding(root);selected=config.get('kernel_profile')
    require(type(selected) is dict and selected==expected,'exact reviewed opt-in profile/manifest/validator binding required')
    image=Path(config['kernel']);require(image==root/RELEASE/'Image','selected Image must be the reviewed profile member')
    for path in [image,Path(selected['manifest']),Path(selected['validator'])]:canonical(path,root)
    validator=_load_validator(Path(selected['validator']))
    require(_base_digest(root)==BASE,'old default Image pin changed')
    value=validator.validate(Path(selected['manifest']),MANIFEST_SHA,image)
    require(type(value) is dict and value.get('status')=='PASS_HOST_PROFILE_ONLY' and value.get('interface')==INTERFACE and
        value.get('profile_id')==PROFILE and value.get('producer_manifest_sha256')==MANIFEST_SHA and
        value.get('Image')==str(image) and value.get('Image_sha256')==IMAGE_SHA and value.get('embedded_config_sha256')==CONFIG_SHA and
        value.get('runtime_tested') is False and value.get('authority_to_boot_or_replace_existing_Image') is False,'actual reviewed profile result/member mismatch')
    return {**value,'external_validator_sha256':VALIDATOR_SHA,'external_validator_path':selected['validator'],
            'old_default_Image_sha256':BASE,'producer_consumer':'reviewed E object/source/compiler/config/link closure; host only'}

"""Test-only process inventories and exact P1 loader/API boundaries."""
from collections import Counter
import ctypes
import hashlib
import json
import os
from pathlib import Path
import stat

SHIM_SHA = '7f1d647e5fdbbba97b79c21271445383a207ac369fe44bc1025af4041fce420e'
TASK_SHA = 'd7945a85cc49aedeed14006f1c7d2077807f6fb5e9565fe21cfa6cb9e694769d'
BUFFER_SHA = 'ee1e5b64d5f836950892fc3d6e986a3eda8d106b7c92f27f8420589184073f96'
_hashes = {}


def sha(path):
    path = Path(path)
    info = path.stat()
    key = (str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    if key not in _hashes:
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024*1024), b''):
                digest.update(chunk)
        _hashes[key] = digest.hexdigest()
    return _hashes[key]


def inventory(pid):
    base = Path('/proc') / str(pid)
    fds = {}
    for entry in (base / 'fd').iterdir():
        try:
            info = entry.stat()
            target = os.readlink(entry)
            details = (base / 'fdinfo' / entry.name).read_text().splitlines()
            flags = int(next(row.split()[1] for row in details if row.startswith('flags:')), 8)
            fds[entry.name] = {'target': target, 'mode': info.st_mode, 'dev': info.st_dev, 'inode': info.st_ino,
                               'char': stat.S_ISCHR(info.st_mode), 'rdev': [os.major(info.st_rdev), os.minor(info.st_rdev)],
                               'flags': flags, 'cloexec': bool(flags & os.O_CLOEXEC),
                               'inheritable': not bool(flags & os.O_CLOEXEC), 'flags_source': 'Linux proc fdinfo'}
        except FileNotFoundError:
            continue  # Preserve all observations which survived this bounded sample.
    maps = (base / 'maps').read_text()
    paths = set()
    for line in maps.splitlines():
        row = line.split(None, 5)
        if len(row) == 6 and row[5].startswith('/') and any(name in row[5] for name in (
                'Ascend', '_task_interface', 'libuf_acl', 'api_fence')):
            paths.add(row[5])
    loaded = {str(Path(path).resolve()): sha(path) for path in sorted(paths)}
    env = dict(value.split('=', 1) for value in (base / 'environ').read_bytes().decode().split('\0') if '=' in value)
    executable = (base / 'exe').resolve()
    return {'pid': pid, 'exe': str(executable), 'exe_sha256': sha(executable), 'fds': fds, 'maps': maps,
            'loaded_hashes': loaded, 'LD_PRELOAD': env.get('LD_PRELOAD'),
            'children': [int(value) for value in (base / 'task' / str(pid) / 'children').read_text().split()]}


def judge(value, role):
    fence = str(Path(os.environ['UFLOW_POOL_TEST_FENCE']).resolve())
    assert value['LD_PRELOAD'] == fence, 'actual process lost the exact test fence'
    assert value['loaded_hashes'].get(fence) == sha(fence)
    controls = [fd for fd in value['fds'].values() if fd['target'].startswith((
        '/dev/davinci', '/dev/devmm', '/dev/hisi', '/dev/ascend')) or fd['char'] and fd['rdev'][0] in (235, 509)]
    if role == 'daemon':
        expected = Counter({('/dev/davinci_manager', True, (235, 0), False): 3,
                            ('/dev/hisi_hdc', True, (509, 0), True): 1})
        actual = Counter((fd['target'], fd['char'], tuple(fd['rdev']), fd['inheritable']) for fd in controls)
        assert actual == expected, {'role': role, 'controls': controls}
        assert any(path.endswith('/libuf_acl_shim.so') and digest == SHIM_SHA for path, digest in value['loaded_hashes'].items())
    else:
        assert controls == [], {'role': role, 'controls': controls}
        if role == 'provider':
            assert any('_task_interface' in path and digest == TASK_SHA for path, digest in value['loaded_hashes'].items())


def binding_proof():
    """Only resolve addresses and record owning files; never call a UF/ACL API."""
    class DlInfo(ctypes.Structure):
        _fields_ = [('filename', ctypes.c_char_p), ('base', ctypes.c_void_p),
                    ('symbol', ctypes.c_char_p), ('address', ctypes.c_void_p)]
    handle = ctypes.CDLL(None)
    handle.dladdr.argtypes = [ctypes.c_void_p, ctypes.POINTER(DlInfo)]
    fence = str(Path(os.environ['UFLOW_POOL_TEST_FENCE']).resolve())
    names = json.loads(Path(os.environ['UFLOW_POOL_TEST_SYMBOLS']).read_text())
    assert len(names) == len(set(names)) == 79 and 'uf_acl_backend_finalize' in names
    bindings = {}
    for name in names:
        info = DlInfo()
        assert handle.dladdr(ctypes.cast(getattr(handle, name), ctypes.c_void_p), ctypes.byref(info))
        bindings[name] = str(Path(os.fsdecode(info.filename)).resolve())
    assert set(bindings.values()) == {fence}
    return bindings


def save_guard(pid, role, phase):
    value = inventory(pid)
    path = Path(os.environ['UFLOW_POOL_TEST_INVENTORY_ROOT']) / f'{phase}-{role}-{pid}.json'
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
    judge(value, role)  # The complete original inventory survives failed policy checks.
    return {'inventory_file': str(path), 'sha256': sha(path), 'pid': pid, 'role': role,
            'exe_sha256': value['exe_sha256'], 'loaded_hashes': value['loaded_hashes']}

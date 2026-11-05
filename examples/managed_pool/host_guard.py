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
_host_identity = None
_CONTROL_NODES = {
    '/dev/davinci_manager': ('hisi_davinci_intf', '/sys/devices/virtual/devdrv_manager/davinci_manager'),
    '/dev/hisi_hdc': ('hisi_hdc', '/sys/devices/virtual/hisi_hdc/hisi_hdc'),
}
_ASCEND_DRIVERS = frozenset(('hisi_davinci_intf', 'hisi_hdc', 'dvpp_cmdlist', 'devmm_svm', 'devdrv-cdev'))


def _character_drivers(raw):
    assert type(raw) is str and len(raw) <= 65536, 'bounded /proc/devices required'
    drivers, character = {}, False
    for line in raw.splitlines():
        if line == 'Character devices:':
            character = True
        elif line == 'Block devices:':
            character = False
        elif character and line.strip():
            parts = line.split()
            assert len(parts) == 2 and parts[0].isdigit(), 'malformed character registration'
            major = int(parts[0])
            names = drivers.setdefault(major, set())
            assert parts[1] not in names, 'duplicate character registration'
            names.add(parts[1])
    assert drivers, 'character registrations missing'
    return drivers


def _read_calibration():
    """Read only kernel metadata of exact canonical nodes, never open a device."""
    raw = {'source_kind': 'runtime-local-kernel', 'proc_devices_path': '/proc/devices',
           'nodes': {}, 'errors': []}
    try:
        raw['proc_devices'] = Path('/proc/devices').read_text()
        raw['proc_devices_sha256'] = hashlib.sha256(raw['proc_devices'].encode()).hexdigest()
        raw['character_drivers'] = {str(k): sorted(v) for k, v in _character_drivers(raw['proc_devices']).items()}
    except (OSError, AssertionError, ValueError) as error:
        raw['errors'].append('/proc/devices: ' + repr(error))
    for path in _CONTROL_NODES:
        node = {'path': path}
        raw['nodes'][path] = node
        try:
            info = os.lstat(path)
            major, minor = os.major(info.st_rdev), os.minor(info.st_rdev)
            node.update(is_char=stat.S_ISCHR(info.st_mode), is_symlink=stat.S_ISLNK(info.st_mode),
                        canonical_path=os.path.realpath(path), st_dev=info.st_dev, st_ino=info.st_ino,
                        st_mode=info.st_mode, rdev=[major, minor])
            node['kernel_driver_names'] = raw.get('character_drivers', {}).get(str(major), [])
            sysnode = Path('/sys/dev/char') / f'{major}:{minor}'
            node.update(sysfs_path=str(sysnode), sysfs_exists=sysnode.exists(),
                        sysfs_realpath=str(sysnode.resolve()))
            node['sysfs_uevent'] = (sysnode / 'uevent').read_text()
            again = os.lstat(path)
            assert (info.st_dev, info.st_ino, info.st_mode, info.st_rdev) == (
                again.st_dev, again.st_ino, again.st_mode, again.st_rdev), 'canonical node changed while sampling'
        except (OSError, AssertionError) as error:
            node['error'] = repr(error)
            raw['errors'].append(path + ': ' + repr(error))
    return raw


def _validated_calibration(raw):
    """Return kernel-derived identity/errors; actor FD values are never inputs."""
    errors = list(raw.get('errors', []))
    identity = {'nodes': {}, 'ascend_character_drivers': {}}
    try:
        drivers = _character_drivers(raw.get('proc_devices'))
        identity['ascend_character_drivers'] = {str(k): sorted(v) for k, v in drivers.items() if v & _ASCEND_DRIVERS}
    except (AssertionError, ValueError) as error:
        errors.append(str(error))
        drivers = {}
    for path, (driver, syspath) in _CONTROL_NODES.items():
        try:
            node = raw.get('nodes', {}).get(path, {})
            assert not node.get('error'), node.get('error')
            assert node.get('is_char') is True and node.get('is_symlink') is False, 'canonical char/non-symlink node required'
            assert node.get('canonical_path') == path, 'canonical device path mismatch'
            assert type(node.get('st_mode')) is int and stat.S_ISCHR(node['st_mode']), 'canonical stat type mismatch'
            assert all(type(node.get(key)) is int and node[key] > 0 for key in ('st_dev', 'st_ino')), 'canonical node identity missing'
            rdev = node.get('rdev')
            assert type(rdev) is list and len(rdev) == 2 and all(type(v) is int and v >= 0 for v in rdev), 'canonical rdev invalid'
            assert rdev[1] == 0 and drivers.get(rdev[0]) == {driver}, 'canonical kernel character driver/minor mismatch'
            assert node.get('sysfs_exists') is True and node.get('sysfs_realpath') == syspath, 'canonical sysfs identity mismatch'
            assert node.get('sysfs_path', f'/sys/dev/char/{rdev[0]}:0') == f'/sys/dev/char/{rdev[0]}:0', 'sysfs char link mismatch'
            uevent = node.get('sysfs_uevent')
            assert type(uevent) is str and len(uevent) <= 65536, 'bounded sysfs uevent missing'
            parsed = {}
            for line in uevent.splitlines():
                key, value = line.split('=', 1)
                assert key not in parsed, 'duplicate sysfs uevent field'
                parsed[key] = value
            assert parsed.get('MAJOR') == str(rdev[0]) and parsed.get('MINOR') == '0', 'sysfs rdev mismatch'
            assert parsed.get('DEVNAME') == path.rsplit('/', 1)[1], 'sysfs DEVNAME mismatch'
            identity['nodes'][path] = {key: node[key] for key in (
                'canonical_path', 'st_dev', 'st_ino', 'st_mode', 'rdev', 'sysfs_realpath', 'sysfs_uevent')}
            identity['nodes'][path]['kernel_driver_name'] = driver
        except (AssertionError, ValueError, TypeError) as error:
            errors.append(path + ': ' + str(error))
    return identity, errors


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
    calibration_before = _read_calibration()
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
    calibration_after = _read_calibration()
    return {'pid': pid, 'exe': str(executable), 'exe_sha256': sha(executable), 'fds': fds, 'maps': maps,
            'device_calibration': {'before': calibration_before, 'after': calibration_after,
                                   'errors': {'before': _validated_calibration(calibration_before)[1],
                                              'after': _validated_calibration(calibration_after)[1]}},
            'loaded_hashes': loaded, 'LD_PRELOAD': env.get('LD_PRELOAD'),
            'children': [int(value) for value in (base / 'task' / str(pid) / 'children').read_text().split()]}


def judge(value, role):
    global _host_identity
    calibration = value.get('device_calibration', {})
    before, before_errors = _validated_calibration(calibration.get('before', {}))
    after, after_errors = _validated_calibration(calibration.get('after', {}))
    assert not before_errors and not after_errors, {'calibration_errors': [before_errors, after_errors]}
    assert before == after, 'related kernel device identity changed during inventory'
    if _host_identity is None:
        _host_identity = before
    assert _host_identity == before, 'related kernel device identity changed between inventories'
    fence = str(Path(os.environ['UFLOW_POOL_TEST_FENCE']).resolve())
    assert value['LD_PRELOAD'] == fence, 'actual process lost the exact test fence'
    assert value['loaded_hashes'].get(fence) == sha(fence)
    controls = [fd for fd in value['fds'].values() if fd['target'].startswith((
        '/dev/davinci', '/dev/devmm', '/dev/hisi', '/dev/ascend')) or fd['char'] and str(fd['rdev'][0]) in before['ascend_character_drivers']]
    if role == 'daemon':
        expected = Counter()
        for path, count, inherit, access in (('/dev/davinci_manager', 3, False, os.O_RDWR),
                                            ('/dev/hisi_hdc', 1, True, os.O_RDONLY)):
            node = before['nodes'][path]
            expected[(path, True, node['st_dev'], node['st_ino'], node['st_mode'], tuple(node['rdev']),
                      access, inherit, not inherit)] = count
        actual = Counter((fd['target'], fd['char'], fd['dev'], fd['inode'], fd['mode'], tuple(fd['rdev']),
                          fd['flags'] & 3, fd['inheritable'], fd['cloexec']) for fd in controls)
        assert all(fd['cloexec'] == bool(fd['flags'] & 0o2000000) for fd in controls), 'FD flags/CLOEXEC mismatch'
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

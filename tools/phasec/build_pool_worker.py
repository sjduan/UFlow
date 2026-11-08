"""Official frozen worker recipe, task-private output, complete compiler closure."""
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

def build_worker(root,out,run):
    lower=root/'src/ub_sim/mem_service';platform=lower.parent
    gva=platform/'guest-linux/aarch64/libs/gva_manager'
    def git(path,*args):return subprocess.check_output(['git','-C',str(path),*args]).decode().strip()
    def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    if git(platform,'rev-parse','HEAD')!='abeec108a1607feb37218d0d89987e37c579d567':
        raise RuntimeError('locked platform base changed; no implicit upgrade')
    if git(lower,'rev-parse','HEAD')!='546985e26bce60798fa3f88329c3fbed8350c798' or git(lower/'vendor/obmm','rev-parse','HEAD')!='53011eed10716b422d2ac29199f68b55f7c5bdc5':
        raise RuntimeError('fixed official worker source pins required')
    work=out/'official-worker';work.mkdir(exist_ok=False)
    compiler='/usr/local/bin/aarch64-linux-gnu-gcc'
    flags='-O2 -Wall -Wextra -static -save-temps=obj -I'+str(root/'env/kernel-uapi/include')+' -Wl,-Map='+str(work/'link.map')
    argv=['make','-f',str(lower/'apps/mem_service/Makefile'),'-j','4',
        'ROOT='+str(lower),'HOST_CC='+compiler,'HOST_CFLAGS='+flags,'GVA_MANAGER_ROOT='+str(gva),
        'MEM_SERVICE_OBMM_BUILD_CONFIG='+str(work/'obmm-provider.build-config'),'linqu_mem_service_provider_obmm']
    dry=subprocess.check_output(argv[:1]+['-n']+argv[1:],cwd=work).decode()
    (work/'recipe.txt').write_text(dry)
    commands=dry.replace('\\\n',' ').splitlines()
    command=next(shlex.split(line) for line in commands if line.startswith(compiler+' ') and '-o linqu_mem_service_provider_obmm' in line)
    run(argv,cwd=work,timeout=300)
    binary=work/'linqu_mem_service_provider_obmm'
    data=binary.read_bytes()
    if data[:4]!=b'\x7fELF' or int.from_bytes(data[18:20],'little')!=183:raise RuntimeError('official worker aarch64 ELF required')
    sources=[Path(p) for p in command if p.endswith('.c')]
    dependencies={lower/'apps/mem_service/Makefile',gva/'gva_manager.c',gva/'gva_manager.h'}
    options=[];i=1
    while i<len(command):
        value=command[i]
        if value=='-o':i+=2;continue
        if not value.endswith('.c') and not value.startswith('-Wl,'):options.append(value)
        i+=1
    for source in sources:
        text=subprocess.check_output([compiler,'-M']+options+[str(source)],cwd=work).decode().replace('\\\n',' ')
        paths=shlex.split(text.split(':',1)[1])
        for name in paths:dependencies.add(Path(name).resolve())
    for name in re.findall(r'^LOAD\s+(\S+)',(work/'link.map').read_text(),re.M):
        path=Path(name);dependencies.add(path.resolve() if path.is_absolute() else (work/path).resolve())
    dependencies.add(Path(compiler).resolve())
    for program in ['cc1','as','collect2','ld']:
        dependencies.add(Path(subprocess.check_output([compiler,'-print-prog-name='+program]).decode().strip()).resolve())
    if len(dependencies)>1024:raise RuntimeError('bounded compiler closure exceeded')
    records={}
    for path in sorted(dependencies):
        if not path.is_file() or path.stat().st_size>128<<20:raise RuntimeError('regular bounded compiler input required: '+str(path))
        entry={'bytes':path.stat().st_size,'sha256':sha(path)}
        if path.is_relative_to(work):entry['compiler_generated_intermediate']=True
        if path.is_relative_to(lower/'vendor/obmm'):
            repository=lower/'vendor/obmm';rel=path.relative_to(repository).as_posix();pin='53011eed10716b422d2ac29199f68b55f7c5bdc5'
        elif path.is_relative_to(lower):
            repository=lower;rel=path.relative_to(repository).as_posix();pin='546985e26bce60798fa3f88329c3fbed8350c798'
        elif path.is_relative_to(platform):
            repository=platform;rel=path.relative_to(repository).as_posix();pin='abeec108a1607feb37218d0d89987e37c579d567'
        else:repository=None
        if repository is not None:
            raw=subprocess.check_output(['git','-C',str(repository),'show',pin+':'+rel])
            if hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise RuntimeError('locked worker input differs from Git: '+rel)
            entry.update(commit=pin,git_blob=git(repository,'rev-parse',pin+':'+rel))
        records[str(path)]=entry
    receipt={'schema':1,'official_recipe':str(lower/'apps/mem_service/Makefile'),'make_argv':argv,
        'expanded_compiler_argv':command,'compiler_target':subprocess.check_output([compiler,'-dumpmachine']).decode().strip(),
        'compiler_version':subprocess.check_output([compiler,'--version']).decode().splitlines()[0],
        'source_and_headers_link_inputs':records,'ELF':str(binary),'ELF_bytes':len(data),'ELF_sha256':sha(binary),
        'link_map_sha256':sha(work/'link.map'),'recipe_sha256':sha(work/'recipe.txt'),
        'managed_worker_enabled':'-DMEM_SERVICE_OBMM_MANAGED_WORKER' in command,
        'official_source_modified':False,'device_or_worker_executed':False}
    if not receipt['managed_worker_enabled']:raise RuntimeError('official managed worker definition absent')
    (work/'receipt.json').write_text(json.dumps(receipt,indent=2))
    return binary,receipt

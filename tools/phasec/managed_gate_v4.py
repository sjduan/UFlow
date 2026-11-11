"""Actual c101/W/R production admission under the compiled new E0 policy."""
from dataclasses import dataclass
import os
from pathlib import Path
from accepted_c101_v4 import v4_anchors,v4_policy,v4_sources,v4_context
from c101_execution_adapter import c101_execution,c101_manifest_gate
from accepted_producers import ap_require
_RUNTIME=object()
@dataclass(frozen=True)
class _RuntimeAdmission:
    context:dict
    anchors:dict
    modules:dict
    profile:dict
    _seal:object

def require_runtime(value):
    ap_require(type(value) is _RuntimeAdmission and value._seal is _RUNTIME,'production-not-ready: exact compiled current-c101 W/R E0 admission required before original/control IO');return value

def runtime_admission(config,build,phase='successor'):
    policy=v4_policy();selection=config.get('terminal_c101_execution')
    ap_require(type(selection) is dict and set(selection)=={'path','sha256'},'production-not-ready: exact current-c101 execution selection required before producer/lower/original IO')
    c101_manifest_gate(selection['path'],selection['sha256'],policy)
    a=v4_anchors();context=c101_execution({'execution_action':phase,'execution_manifest':selection,'actor_pid':os.getpid()},a,policy)
    ap_require(build['source']==context['source'] and build['source_commit']==context['plan']['source_commit'],'actual C driver/source/runtime binding')
    modules=v4_sources(a);anchor,profile,_=v4_context(a,modules)
    ap_require(config['managed_write_profile']==context['group']['managed_write_profile'] and config['qemu_binary']==profile['qemu_binary'],'exact accepted c1e W/R profile selection')
    return _RuntimeAdmission(context,a,modules,profile,_RUNTIME)

def selected_identity(admission):
    admission=require_runtime(admission)
    from qemu_profile_gate import _identity,_PRODUCTION
    a=admission.anchors;d=a['code_and_data'];profile=admission.profile
    return _identity(d['qemu_binary']['path'],d['qemu_binary']['sha256'],{**profile,'external_validator':d['profile_validator'],'profile_manifest':d['profile'],'accepted_context':a['accepted_context'],'HOST_fixture_only':False,'boot_authority':False,'managed_v4':True},_PRODUCTION)

def capture_binding(config,identity):
    a=v4_anchors();d=a['code_and_data'];ap_require(identity.metadata().get('managed_v4') is True and identity.metadata().get('accepted_context')==a['accepted_context'] and identity.metadata().get('profile_manifest')==d['profile'],'actual accepted managed c101 W/R identity required')
    ap_require(config.get('managed_write_profile')=={'manifest':d['profile']['path'],'manifest_sha256':d['profile']['sha256'],'validator_sha256':d['profile_validator']['sha256']} and config.get('qemu_binary')==str(identity.binary),'capture actual c1e W/R profile/binary drift')
    return identity

"""Compiled-anchor production admission, actual release-v2 profile identity."""
from dataclasses import dataclass
import os
from pathlib import Path
from accepted_c081_v3 import v3_anchors,v3_policy,v3_sources,v3_context,V3_SHA
from c081_execution_adapter import c081_execution,c081_manifest_gate
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
    ap_require(type(value) is _RuntimeAdmission and value._seal is _RUNTIME,'production-not-ready: actual compiled accepted-context E0 admission required before original/control IO');return value
def runtime_admission(config,build,phase='successor'):
    policy=v3_policy();selection=config.get('terminal_case_execution')
    ap_require(type(selection) is dict and set(selection)=={'path','sha256'},'production-not-ready: exact v3 execution selection required before producer/lower/original IO')
    c081_manifest_gate(selection['path'],selection['sha256'],policy)
    a=v3_anchors();context=c081_execution({'execution_action':phase,'execution_manifest':selection,'actor_pid':os.getpid()},a,policy)
    ap_require(build['source']==context['source'] and build['source_commit']==context['plan']['source_commit'],'actual driver/source/runtime binding')
    modules=v3_sources(a);anchor,profile,_=v3_context(a,modules)
    ap_require(config['managed_cache_profile']==context['group']['managed_cache_profile'] and config['qemu_binary']==profile['qemu_binary'],'exact actual accepted managed profile selection')
    return _RuntimeAdmission(context,a,modules,profile,_RUNTIME)
def selected_identity(admission):
    admission=require_runtime(admission)
    from qemu_profile_gate import _identity,_PRODUCTION
    a=admission.anchors;profile=admission.profile
    return _identity(a['code_and_data']['qemu_binary']['path'],a['code_and_data']['qemu_binary']['sha256'],{**profile,'external_validator':a['code_and_data']['profile_validator'],'profile_manifest':a['code_and_data']['profile'],'accepted_context':a['accepted_context'],'HOST_fixture_only':False,'boot_authority':False,'managed_v3':True},_PRODUCTION)
def capture_binding(config,identity):
    a=v3_anchors();ap_require(identity.metadata().get('managed_v3') is True and identity.metadata().get('accepted_context')==a['accepted_context'] and identity.metadata().get('profile_manifest')==a['code_and_data']['profile'],'actual accepted managed v6 identity required')
    ap_require(config.get('managed_cache_profile')=={'manifest':a['code_and_data']['profile']['path'],'manifest_sha256':a['code_and_data']['profile']['sha256'],'validator_sha256':a['code_and_data']['profile_validator']['sha256']} and config.get('qemu_binary')==str(identity.binary),'capture actual managed profile/binary drift')
    return identity

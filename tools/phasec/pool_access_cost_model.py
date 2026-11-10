"""Source/counter lower-bound model, never a benchmark or live-run permit."""
import argparse,hashlib,json
from pathlib import Path
SDK_SHA='d72065ecf6c2c0d866ab1017dfa5654621c1e2307e8c6d4ceb9bb38dcb5766af'

def model(nbytes,calls=2260210,seconds=120.525935305):
    if type(nbytes) is not int or nbytes<=0 or calls<=0 or seconds<=0:raise ValueError('positive bounded workload/counter interval required')
    # invalidate_range and wait_range_visible each perform an SDK checksum;
    # the callback independently checks every byte and then hashes every byte.
    rate=calls/seconds
    return {'nbytes':nbytes,'SDK_mapped_checksum_passes_before_callback':2,'callback_byte_verify_passes':1,'callback_checksum_passes':1,
        'minimum_scalar_read_calls':4*nbytes,'before_callback_scalar_read_calls':2*nbytes,
        'optimistic_aggregate_counter_calls_per_second':rate,'one_scalar_pass_estimated_seconds':nbytes/rate,
        'all_four_passes_estimated_seconds':4*nbytes/rate,'unchanged_SDK_two_pass_floor_seconds':2*nbytes/rate,
        'C_only_eight_byte_callback_loads_still_scalar_SDK_estimated_seconds':(2+2/8)*nbytes/rate,
        'isolated_benchmark':False,'additional_provider_coherence_work_ignored':True,
        'counter_includes_startup_and_64KiB_so_rate_is_not_an_isolated_20MiB_measurement':True,
        'exact_future_environment_speedup_claimed':False,'SDK_or_visibility_hash_byte_guards_removed':False}

def source_model(sdk_path,source):
    raw=Path(sdk_path).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=SDK_SHA:raise ValueError('fixed SDK53 provider source drift')
    sdk=raw.decode();start=sdk.index('static int mem_service_provider_channel_mapping_range(');end=sdk.index('\nint mem_service_provider_channel_publish_range(',start);mapping=sdk[start:end]
    required=['operation == MEM_SERVICE_MAPPING_RANGE_PUBLISH','mem_service_provider_checksum64(bytes, len) != expected_checksum',
        'operation == MEM_SERVICE_MAPPING_RANGE_INVALIDATE','operation == MEM_SERVICE_MAPPING_RANGE_WAIT_VISIBLE','completion.visible_bytes != len','completion.checksum != expected_checksum']
    if not all(s in mapping for s in required) or mapping.count('mem_service_provider_checksum64(bytes, len)')!=2:raise ValueError('SDK range/checksum pipeline changed')
    client=Path(source)/'crates/uf-ub-sim-agent/c/pool_client.c';body=client.read_text();start=body.index('static int access_data(');end=body.index('\nstatic int wait_state(',start);access=body[start:end]
    for item in ['mem_service_provider_channel_invalidate_range','mem_service_provider_channel_wait_range_visible',
        'for(uint64_t i=0;i<job->nbytes;i++)if(bytes[i]!=pattern','mem_service_provider_checksum64(bytes,job->nbytes)']:
        if item not in access:raise ValueError('application full-byte/SDK visibility pipeline changed')
    return {'schema':1,'kind':'HOSTSourceCounterPipelineModelNotBenchmark','SDK_provider_source_sha256':SDK_SHA,
        'client_source_sha256':hashlib.sha256(client.read_bytes()).hexdigest(),'SDK_mapping_function_sha256':hashlib.sha256(mapping.encode()).hexdigest(),
        'client_access_function_sha256':hashlib.sha256(access.encode()).hexdigest(),'read20MiB':model(20<<20),'read64KiB':model(65536),
        'conclusion':'C-only callback widening or log suppression cannot remove the two preceding scalar SDK scans; require coherent environment path benefiting fixed SDK, without weakening any guard',
        'C_callback_optimization_implemented':False,'SDK53_modified':False,'actual_Guest_performance_pass_claimed':False}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--sdk-provider',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    value=source_model(a.sdk_provider,a.source)
    with a.output.open('x') as f:json.dump(value,f,indent=2)
    print(json.dumps(value,indent=2))

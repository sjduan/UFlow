"""Actual reviewed E profile HOST positive/negative and no-mutation ordering.
Private malformed fixtures are negative inputs, never a production bypass.
"""
import argparse,copy,gzip,hashlib,json,os,sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
import kernel_profile_gate as gate
import run_mem_service_pool_matrix as driver

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--probe-root',type=Path,required=True);a=p.parse_args()
 if 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted HOST build required')
 assert a.probe_root.parent.is_dir() and a.probe_root.parent.parent==a.root/'runs' and a.probe_root.parent.name.startswith('cb')
 assert a.output.is_relative_to(a.root/'implementation') and a.source.is_relative_to(a.root/'implementation')
 a.output.mkdir();a.probe_root.mkdir();cases=[]
 selected=gate.binding(a.root);image=a.root/gate.RELEASE/'Image';validator=gate._load_validator(Path(selected['validator']))
 manifest=validator.json_bytes(validator.read_file(selected['manifest'],256<<10))
 records={name:dict(row) for name,row in manifest['outputs'].items()}
 protected={row['path']:{'bytes':Path(row['path']).stat().st_size,'sha256':row['sha256']} for row in records.values()}
 protected[selected['validator']]={'bytes':14204,'sha256':gate.VALIDATOR_SHA}
 protected[selected['manifest']]={'bytes':Path(selected['manifest']).stat().st_size,'sha256':gate.MANIFEST_SHA}
 def refused(name,call):
  try:call()
  except (ValueError,KeyError,TypeError,OSError) as error:
   cases.append({'case':name,'pass':True,'expected':'rejected','error_type':type(error).__name__,'reason':str(error)[:600]});return
  raise AssertionError('negative accepted: '+name)
 config={'root':str(a.root),'output':str(a.probe_root/'unused-positive'),'kernel':str(image),'kernel_profile':selected}
 positive=gate.launch_gate(a.root,config);assert positive['runtime_tested'] is False and positive['authority_to_boot_or_replace_existing_Image'] is False
 cases.append({'case':'actual-reviewed-producer-consumer','pass':True,'result':positive})
 for key in ['manifest_sha256','validator_sha256','manifest','validator']:
  c=copy.deepcopy(config);c['kernel_profile'][key]='0'*64 if key.endswith('sha256') else c['kernel_profile'][key]+'.unbound'
  refused('binding-'+key,lambda c=c:gate.launch_gate(a.root,c))
 old=a.root/'src/ub_sim/guest-linux/aarch64/out/Image';assert sha(old)==gate.BASE
 refused('actual-old-disabled-Image',lambda:gate.launch_gate(a.root,dict(config,kernel=str(old))))
 old_config=validator.parse_config(validator.extract_ikconfig(validator.read_file(old,validator.MAX_IMAGE)));assert old_config[validator.FLAG]=='n'
 bad_validator=a.output/'untrusted-validator.py';bad_validator.write_text('raise RuntimeError("unreviewed code must never execute")\n')
 refused('validator-external-file-hash',lambda:gate._load_validator(bad_validator))
 link=a.output/'validator-symlink';link.symlink_to(selected['validator']);refused('validator-nofollow',lambda:gate._load_validator(link))
 refused('public-validator-wrong-external-manifest-pin',lambda:validator.validate(selected['manifest'],'0'*64,image))
 # A rehashed private manifest is only a parser negative fixture; the launch
 # wrapper never accepts its digest/path because its external pins are fixed.
 def modified(name,change,selected_image=None):
  m=copy.deepcopy(manifest);change(m);f=a.output/(name+'.json');raw=json.dumps(m).encode();f.write_bytes(raw)
  refused(name,lambda:validator.validate(f,hashlib.sha256(raw).hexdigest(),selected_image or image))
 for key,value in [('schema',True),('profile_id','wrong'),('interface','wrong'),('kernel_commit','0'*40),('runtime_tested',True),('authority_to_boot_or_replace_existing_Image',True)]:
  modified('actual-provenance-'+key,lambda m,k=key,v=value:m.update({k:v}))
 modified('actual-Image-hash',lambda m:m['outputs']['Image'].update(sha256='0'*64))
 modified('missing-artifact',lambda m:m['outputs'].pop('core_matt.o'))
 source=validator.json_bytes(validator.bound_bytes(records['source_manifest'],1<<20));assert 'tracked_objects' in source and 'files' not in source
 source_bad=dict(source,kernel_commit='0'*40);source_path=a.output/'source-manifest-wrong.json';source_path.write_text(json.dumps(source_bad))
 def bad_source(m):
  m['outputs']['source_manifest'].update(path=str(source_path),bytes=source_path.stat().st_size,sha256=sha(source_path));m['source_manifest_sha256']=sha(source_path)
 modified('actual-source-object-closure',bad_source)
 config_raw=validator.bound_bytes(records['.config'],validator.MAX_CONFIG)
 header=bytearray(64);header[56:60]=b'ARM\x64'
 def negative_image(name,payload):
  f=a.output/(name+'.Image');f.write_bytes(payload)
  modified(name,lambda m:m['outputs']['Image'].update(path=str(f),bytes=f.stat().st_size,sha256=sha(f)),f)
 packed=gzip.compress(config_raw)
 negative_image('truncated-actual-embedded-config',bytes(header)+b'IKCFG_ST'+packed[:-3]+b'IKCFG_ED')
 negative_image('duplicate-actual-config-symbol',bytes(header)+b'IKCFG_ST'+gzip.compress(config_raw+b'CONFIG_UB_UMMU_SVA_SEPARATED_PAGES=y\n')+b'IKCFG_ED')
 negative_image('oversized-embedded-config',bytes(header)+b'IKCFG_ST'+gzip.compress(b'#'*(validator.MAX_CONFIG+1))+b'IKCFG_ED')
 negative_image('malformed-embedded-config',bytes(header)+b'IKCFG_ST'+gzip.compress(b'CONFIG_BROKEN\n')+b'IKCFG_ED')
 duplicate=a.output/'duplicate-manifest.json';duplicate.write_bytes(b'{"schema":1,"schema":1}')
 refused('duplicate-manifest-keys',lambda:validator.validate(duplicate,sha(duplicate),image))
 order_cases=[]
 for name,c in [('missing-binding',dict(config,kernel_profile={})),('old-image',dict(config,kernel=str(old))),('wrong-external-pin',dict(config,kernel_profile=dict(selected,manifest_sha256='0'*64)))]:
  out=a.probe_root/name;out.mkdir();c['output']=str(out)
  with patch.object(driver.subprocess,'Popen',side_effect=AssertionError('a process started before a failed profile gate')) as popen:
   refused('matrix-order-'+name,lambda:driver.matrix(a.root,c,{'source':str(a.source)}))
   popen.assert_not_called()
  assert list(out.iterdir())==[];order_cases.append(name)
 # Recheck every producer output after all negative fixtures. Nothing in E's
 # release or protected old baseline is changed or rewritten to fit a test.
 for path,row in protected.items():
  f=Path(path);assert f.is_file() and not f.is_symlink() and f.stat().st_size==row['bytes'] and sha(f)==row['sha256']
 assert sha(old)==gate.BASE
 result={'pass':True,'actual_reviewed_E_profile_consumed':True,'producer_manifest_sha256':gate.MANIFEST_SHA,'validator_external_sha256':gate.VALIDATOR_SHA,
  'actual_Image_sha256':gate.IMAGE_SHA,'actual_embedded_config_sha256':gate.CONFIG_SHA,'source_schema':'kernel_commit/tree/tracked_objects/git_ls_tree_sha256/source_root, no fabricated files array',
  'positive':positive,'cases':cases,'case_count':len(cases),'no_process_or_lower_Catalog_lineage_side_effect_order':order_cases,
  'producer_outputs_unchanged':len(protected),'old_default_Image_unchanged':True,'runtime_tested':False,'production_quarantine_or_Guest_executed':False}
 (a.output/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
if __name__=='__main__':main()

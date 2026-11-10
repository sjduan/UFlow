"""One manifest-bound terminal-only CLI invocation; no retry or fallback."""
import hashlib,os,subprocess,sys
from pathlib import Path
from accepted_c101_v4 import v4_anchors,v4_policy
from c101_execution_adapter import c101_execution
def main(path):
    path=Path(path);a=v4_anchors();policy=v4_policy()
    # Namespace check precedes the first supplied manifest read.
    if path!=Path(policy['config_directory'])/'execution.json':raise ValueError('production-not-ready: exact compiled execution path required before IO')
    pin=hashlib.sha256(path.read_bytes()).hexdigest();context=c101_execution({'execution_action':'terminal-only','execution_manifest':{'path':str(path),'sha256':pin},'actor_pid':os.getpid()},a,policy)
    daemon=context['plan']['daemon']['path'];fd=int(os.environ['UFLOW_ENV_CLOSE_FD'])
    result=subprocess.run([daemon,'--pool-terminal-c101-manifest',str(path),pin],pass_fds=(fd,),check=False)
    if result.returncode:raise RuntimeError('actual terminal phase failed; original verdict retained; no retry')
if __name__=='__main__':main(sys.argv[1])

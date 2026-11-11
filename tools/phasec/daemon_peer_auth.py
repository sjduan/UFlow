"""Authenticate direct Catalog UnixStream peers, never forwarded client flags."""
from dataclasses import dataclass
import hashlib,os,re,select,socket,stat,struct
from pathlib import Path
from collect_owned_qemu_metadata import bounded,process_identity

class PeerRejected(ValueError):pass

@dataclass(frozen=True)
class AuthenticatedPeer:
    _owner:object
    def recheck(self):self._owner.recheck()
def require(ok,why):
    if not ok:raise PeerRejected(why)
def file_identity(info):return (info.st_dev,info.st_ino,info.st_uid,info.st_mode,info.st_size,info.st_mtime_ns,info.st_ctime_ns,info.st_nlink)
def observe(pid):
    proc=Path('/proc')/str(pid);identity=process_identity(pid);status=bounded(proc/'status',65536).decode('ascii')
    match=re.search(r'^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)',status,re.M)
    require(match is not None and len(set(match.groups()))==1,'Catalog UID transition rejected')
    identity['uid']=int(match[1]);identity['namespaces']={key:os.readlink(proc/'ns'/key) for key in ('pid','mnt','user')}
    identity['executable']=os.readlink(proc/'exe');require(not identity['executable'].endswith(' (deleted)'),'deleted Catalog executable rejected')
    identity['executable_identity']=file_identity((proc/'exe').stat());require(process_identity(pid)=={k:identity[k] for k in ('pid','birth','ppid','session')},'Catalog changed during observation')
    return identity

@dataclass
class DaemonPeer:
    _expected:dict
    _pidfd:int
    @classmethod
    def capture(cls,pid,executable,external_sha):
        require(type(pid) is int and pid>0,'actual direct Catalog PID required')
        before=observe(pid);path=Path(executable)
        require(before['ppid']==os.getpid() and before['uid']==os.geteuid() and
            all(v==os.readlink(Path('/proc/self/ns')/k) for k,v in before['namespaces'].items()),'actual owned Catalog parent/UID/namespace required')
        require(path.resolve()==path and before['executable']==str(path) and before['executable_identity']==file_identity(path.stat()) and path.stat().st_nlink==1,'single-link exact Catalog executable required')
        require(type(external_sha) is str and re.fullmatch('[0-9a-f]{64}',external_sha),'external runtime ELF pin required')
        # Hash code once at capture, then recheck its complete stable file
        # identity per connection. Do not reread a 40MB ELF for every SDK call.
        with (Path('/proc')/str(pid)/'exe').open('rb') as f:raw=f.read((256<<20)+1)
        require(0<len(raw)<=256<<20 and hashlib.sha256(raw).hexdigest()==external_sha,'actual Catalog code pin mismatch')
        fd=os.pidfd_open(pid,0)
        try:
            require(observe(pid)==before and not select.select([fd],[],[],0)[0],'Catalog birth/exec changed during pinning')
            return cls(before,fd)
        except BaseException:os.close(fd);raise
    def verify(self,connection):
        pid,uid,_=struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
        require((pid,uid)==(self._expected['pid'],self._expected['uid']),'non-Catalog Unix peer rejected')
        self.recheck()
        return AuthenticatedPeer(self)
    def recheck(self):
        require(self._pidfd>=0 and not select.select([self._pidfd],[],[],0)[0],'Catalog died; death is not native revoke ACK')
        require(observe(self._expected['pid'])==self._expected,'Catalog PID/birth/UID/namespace/executable changed')
    def close(self):
        if self._pidfd>=0:os.close(self._pidfd);self._pidfd=-1

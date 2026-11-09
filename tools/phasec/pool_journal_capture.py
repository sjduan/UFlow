"""Reconstruct only known bounded journal serial records; never execute/replay."""
import hashlib,re
MAX_BYTES=65536
def capture(text):
    rows=[s for s in text.splitlines() if s.startswith('UFPOOL_JOURNAL_')]
    if not rows:return {'status':'missing-serial-records','complete':False,'bytes':0},b''
    raw=bytearray();begin=None;end=None
    try:
        for line in rows:
            kind,*parts=line.split();fields=dict(p.split('=',1) for p in parts)
            if kind=='UFPOOL_JOURNAL_BEGIN':
                if begin is not None or fields['source']!='worker.conf.state' or int(fields['max_bytes'])!=MAX_BYTES:raise ValueError()
                begin=fields
            elif kind=='UFPOOL_JOURNAL_CHUNK':
                if begin is None or end is not None or int(fields['offset'])!=len(raw) or not re.fullmatch('[0-9a-f]{2,512}',fields['hex']):raise ValueError()
                b=bytes.fromhex(fields['hex']);raw.extend(b)
                if len(raw)>MAX_BYTES:raise ValueError()
            elif kind=='UFPOOL_JOURNAL_END':
                if end is not None:raise ValueError()
                end=fields
            else:raise ValueError()
        if end is None:return {'status':'incomplete-serial-records','complete':False,'bytes':len(raw)},bytes(raw)
        if int(end['bytes'])!=len(raw):raise ValueError()
        h=1469598103934665603
        for b in raw:h=((h^b)*1099511628211)&((1<<64)-1)
        if begin is not None and end.get('fnv64')!=f'{h:016x}':raise ValueError()
        complete=end['status']=='complete' and end['complete']=='1' and begin is not None and int(begin['file_bytes'])==len(raw)
        if end.get('complete')=='1' and not complete:raise ValueError()
        return {'status':end['status'],'complete':complete,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'source_identity':begin,'errno':int(end['errno']),'SDK_ACK_or_replay_claimed':False},bytes(raw)
    except (ValueError,KeyError,OverflowError):
        return {'status':'invalid-serial-records','complete':False,'bytes':min(len(raw),MAX_BYTES)},bytes(raw[:MAX_BYTES])

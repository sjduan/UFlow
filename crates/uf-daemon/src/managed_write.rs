//! Exclusive Executing-W/Catalog-Writing authority and independent obligations.
//! Native execution is selected only after an independently accepted profile;
//! no client JSON, hash, process death or SDK lease bookkeeping is an ACK.
use crate::{catalog::Catalog,managed_pool::Pool,pool_ranges::RangeLease};
use crate::ddr_recovery_coverage::digest;
use serde_json::{json,Value};
use std::collections::BTreeMap;

#[derive(Clone,Debug)]
pub(crate) struct Authority {body:Value,generation:u64,offset:u64,nbytes:u64,client:String,lease:String}
impl Authority {
 pub(crate) fn mint(pool:&Pool,st:&Catalog,lease:&RangeLease,node:&Value,realm:&str,operation:&str,generation:u64)->Result<Self,String>{
  pool.admission()?;
  pool.ranges.cache.guard_writer(lease.offset,lease.nbytes)?;
  pool.ranges.write.guard_access(lease.offset,lease.nbytes)?;
  let actual=pool.ranges.leases.get(&lease.token).ok_or("actual Catalog range lease missing")?;
  let client=pool.clients.get(&lease.client).ok_or("actual mapped client missing")?;
  let binding=st.access_bindings.get(&client.access_id).ok_or("actual access binding missing")?;
  let catalog=st.leases.get(&lease.catalog_id).ok_or("actual Catalog lease generation missing")?;
  if actual.identity()!=lease.identity()||actual.state!="Executing"||lease.mode!="w"||lease.version!=0||!lease.content.is_empty()||client.access!="rw"||
   client.state!="Mapped"||client.closing||binding.state!="Mapped"||Some(binding.accessor_node_id.as_str())!=node["node"].as_str()||
   catalog.object_id!=pool.object||catalog.client_id!=client.catalog_id||catalog.block_id!=lease.block||catalog.state!="Writing"||catalog.allowed_offset_bytes!=lease.offset||catalog.allowed_bytes!=lease.nbytes||
   generation==0||operation.is_empty()||lease.nbytes==0||lease.offset.checked_add(lease.nbytes).is_none_or(|end|end>pool.nbytes)||
   node["boot_id"].as_str().is_none_or(str::is_empty)||node["agent_session"].as_str().is_none_or(str::is_empty)||node["incarnation"].as_u64().is_none_or(|n|n==0){return Err("server validated Executing W/Catalog Writing/RW client required".into());}
  let ledger=st.ddr_resources.snapshot();let resource=ledger["resources"].as_array().ok_or("actual DDR resources")?.iter().find(|r|r["object_id"]==pool.object).ok_or("actual charged backing missing")?;
  let b=&resource["backing"];
  let rb=st.resource_bindings.get(&pool.binding).ok_or("actual resource binding missing")?;
  if b["kind"]!="provider"||b["provider"]!="mem_service_obmm"||b["realm"]!=realm||b["resource"].as_str().is_none_or(str::is_empty)||b["generation"]!=pool.canonical["generation"]||resource["nbytes"]!=pool.nbytes||
   rb.provider_resource_id!=pool.id||rb.provider_session_id!=pool.provider_session||Some(rb.binding_generation)!=b["generation"].as_u64(){return Err("actual backing identity disagreement".into());}
  let mut body=json!({"schema":2,"op":"write","client_session":lease.client,"offset":lease.offset,"nbytes":lease.nbytes,
   "lease_id":lease.token,"content_version":lease.version,
   "write_version":1,"write_pool_id":pool.id,"write_pool_epoch":pool.epoch,"write_provider_session":pool.provider_session,
   "write_resource_id":pool.object,"write_resource_generation":pool.canonical["generation"],"write_placement_id":pool.placement,"write_binding_id":pool.binding,
   "write_backing_realm":b["realm"],"write_backing_provider":b["provider"],"write_backing_resource":b["resource"],"write_backing_generation":b["generation"],
   "write_access_generation":client.access_id,"write_lease_generation":lease.catalog_id,"write_block_id":lease.block,"write_block_generation":lease.generation,
   "write_mode":"w","write_rights":"rw","write_operation_id":operation,"write_control_generation":generation});
  // The frame tuple is provided by the server's already bound logical node.
  let frame=json!({"request":generation,"correlation":generation,"realm":realm,"node":node["node"],"boot":node["boot_id"],"session":node["agent_session"],"generation":node["incarnation"]});
  body["write_frame_sha256"]=json!(digest(&serde_json::to_vec(&frame).map_err(|e|e.to_string())?));
  Ok(Self{body,generation,offset:lease.offset,nbytes:lease.nbytes,client:lease.client.clone(),lease:lease.token.clone()})
 }
 pub(crate) fn request(&self,pattern:&Value)->Result<Value,String>{
  if pattern.as_u64().is_none(){return Err("typed pattern required".into());}
  let mut body=self.body.clone();body["pattern_id"]=pattern.clone();
  body["write_authority_sha256"]=json!(digest(&serde_json::to_vec(&body).map_err(|e|e.to_string())?));
  if serde_json::to_vec(&body).map_err(|e|e.to_string())?.len()>1800{return Err("managed authority exceeds unchanged serial metadata bound".into());}Ok(body)
 }
}
#[derive(Clone,Debug)]
struct Obligation {authority:Authority,request:Value,state:&'static str}
#[derive(Default)]
pub(crate) struct Book {live:BTreeMap<u64,Obligation>,highwater:u64}
impl Book {
 pub(crate) fn begin(&mut self,authority:Authority,pattern:&Value)->Result<Value,String>{
  if authority.generation<=self.highwater||self.live.len()>=64||self.live.values().any(|g|g.authority.lease==authority.lease){return Err("managed W control generation replay/live grant bound".into());}
  self.guard_access(authority.offset,authority.nbytes)?;
  let request=authority.request(pattern)?;self.highwater=authority.generation;self.live.insert(authority.generation,Obligation{authority,request:request.clone(),state:"BeginPending"});Ok(request)
 }
 pub(crate) fn finish(&mut self,generation:u64,receipt:&Value)->Result<(),String>{
  let g=self.live.get(&generation).ok_or("grant missing; no fabricated end ACK")?;
  if g.state=="Unknown"||receipt["write_version"].as_u64()!=Some(1)||receipt["write_control_generation"].as_u64()!=Some(generation)||
   receipt["write_authority_sha256"]!=g.request["write_authority_sha256"]||receipt["write_frame_sha256"]!=g.request["write_frame_sha256"]||
   receipt["write_begin_confirmed"]!=true||receipt["write_end_confirmed"]!=true||receipt["write_active"]!=false||
   receipt["write_in_flight"].as_u64()!=Some(0)||receipt["write_pages"].as_u64()!=Some(0)||
   receipt["write_dirty_bytes"].as_u64()!=Some(0)||receipt["write_flush_confirmed"]!=true||
   receipt["write_transcript_sha256"].as_str().is_none_or(|s|s.len()!=64||!s.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b))) {
    self.uncertain(generation);return Err("native write/verification/flush/end uncertainty retains W obligation".into());
  }
  self.live.remove(&generation);Ok(())
 }
 pub(crate) fn uncertain(&mut self,generation:u64){if let Some(g)=self.live.get_mut(&generation){g.state="Unknown";}}
 pub(crate) fn has_unknown(&self)->bool{self.live.values().any(|g|g.state=="Unknown")}
 pub(crate) fn is_empty(&self)->bool{self.live.is_empty()}
 pub(crate) fn has_history(&self)->bool{self.highwater!=0}
 pub(crate) fn client_pending(&self,client:&str)->bool{self.live.values().any(|g|g.authority.client==client)}
 pub(crate) fn lease_pending(&self,lease:&str)->bool{self.live.values().any(|g|g.authority.lease==lease)}
 pub(crate) fn guard_access(&self,offset:u64,nbytes:u64)->Result<(),String>{
  let end=offset.checked_add(nbytes).ok_or("write range overflow")?;
  if self.live.values().any(|g|offset<g.authority.offset+g.authority.nbytes && g.authority.offset<end){return Err("live/revoking/uncertain W grant blocks overlapping reader or writer".into());}Ok(())
 }
 pub(crate) fn snapshot(&self)->Value{json!({"control_highwater":self.highwater,"outstanding":self.live.values().map(|g|json!({"authority":g.request,"state":g.state})).collect::<Vec<_>>()})}
}

#[cfg(test)]pub(crate) mod tests;

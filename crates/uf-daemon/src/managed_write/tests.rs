use super::*;
use crate::managed_pool::{Peer,State};

pub(crate) fn fixture()->(Catalog,State,RangeLease,Value){
 let(mut st,mut state,read,node)=crate::managed_read_cache::tests::fixture();
 let p=&state.pools["pool"];let access=p.clients[&read.client].access_id;
 let pid=std::process::id() as i32;let peer=Peer{birth:crate::ddr_process_identity::observe(pid,unsafe{libc::geteuid()},unsafe{libc::getegid()}).unwrap(),pidfd:crate::pool_provider::pidfd(pid).unwrap()};
 let q=json!({"op":"AcquirePoolRange","pool_id":"pool","pool_epoch":"epoch","provider_session":"provider","client_session":read.client,"access_generation":access,
  "block":p.block_json(p.ranges.blocks[&read.block].json()),"mode":"w","expected_generation":1,"byte_offset":65536,"nbytes":65536});
 let id=state.metadata(&mut st,&peer,&q).unwrap();let w=state.pools.get_mut("pool").unwrap().ranges.leases.get_mut(id["lease_id"].as_str().unwrap()).unwrap();w.state="Executing";let w=w.clone();
 (st,state,w,node)
}
fn grant(st:&Catalog,state:&State,l:&RangeLease,node:&Value,g:u64)->Authority{Authority::mint(&state.pools["pool"],st,l,node,"realm","actual-w-operation",g).unwrap()}
pub(crate) fn ack(r:&Value)->Value{json!({"write_version":1,"write_control_generation":r["write_control_generation"],"write_authority_sha256":r["write_authority_sha256"],"write_frame_sha256":r["write_frame_sha256"],"write_begin_confirmed":true,"write_end_confirmed":true,"write_flush_confirmed":true,"write_active":false,"write_in_flight":0,"write_pages":0,"write_dirty_bytes":0,"write_transcript_sha256":"cd".repeat(32)})}

#[test]fn actual_catalog_writing_mints_without_prior_content_and_cannot_be_r(){
 let(st,state,l,node)=fixture();let request=grant(&st,&state,&l,&node,9).request(&json!(29)).unwrap();
 assert_eq!(request["op"],"write");assert_eq!(request["write_rights"],"rw");assert_eq!(request["content_version"],0);assert!(request.get("write_content_id").is_none());assert!(request.get("cache_version").is_none());
 assert_eq!(st.leases[&l.catalog_id].state,"Writing");assert!(crate::managed_read_cache::Authority::mint(&state.pools["pool"],&st,&l,&node,"realm","op",9).is_err());
 assert!(serde_json::to_vec(&request).unwrap().len()<=1800);
}
#[test]fn stale_readonly_committed_cross_node_and_nonexecuting_w_cannot_mint(){
 for change in 0..9 {
  let(mut st,mut state,mut l,mut node)=fixture();let p=state.pools.get_mut("pool").unwrap();
  match change {0=>p.clients.get_mut(&l.client).unwrap().access="ro".into(),1=>st.leases.get_mut(&l.catalog_id).unwrap().state="Reading".into(),2=>p.ranges.leases.get_mut(&l.token).unwrap().state="Active",3=>l.version=1,4=>l.content="invented".into(),5=>node["node"]=json!("nodeA"),6=>st.resource_bindings.get_mut(&p.binding).unwrap().binding_generation+=1,7=>st.leases.get_mut(&l.catalog_id).unwrap().allowed_bytes-=1,_=>p.clients.get_mut(&l.client).unwrap().closing=true}
  assert!(Authority::mint(p,&st,&l,&node,"realm","op",9).is_err(),"change {change}");
 }
}
#[test]fn independent_w_blocks_reader_writer_commit_unmap_close_even_if_lease_book_is_closed(){
 let(mut st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();p.ranges.write.begin(a,&json!(29)).unwrap();
 p.ranges.leases.get_mut(&l.token).unwrap().state="Closed";p.clients.get_mut(&l.client).unwrap().closing=true;
 for mode in ["r","w"] {assert!(p.ranges.acquire(mode.into(),999,l.client.clone(),l.block,l.generation,65536,65536,mode).is_err());}
 assert!(p.ranges.write.guard_access(0,65536).is_ok());assert!(p.ranges.write.guard_access(65536,65536).is_err());
 assert!(p.ranges.commit(&l,"bad-early",65536).is_err());assert!(p.ranges.release(&l).is_err());p.ranges.reconcile_unmapped(&l.client);assert!(p.confirm_client_unmapped(&mut st,&l.client).is_err());assert!(p.confirm_closed(&st).is_err());
 p.ranges.write.uncertain(9);assert!(p.frozen());assert_eq!(st.ddr_resources.charged_bytes(),131072);
}
#[test]fn flush_end_ack_is_typed_exact_sticky_and_not_process_death(){
 for(key,value)in [("write_flush_confirmed",json!(false)),("write_end_confirmed",json!(false)),("write_dirty_bytes",json!(1)),("write_dirty_bytes",json!(false)),("write_in_flight",json!(1)),("write_pages",json!(1)),("write_active",json!(true)),("write_authority_sha256",json!("00".repeat(32))),("write_frame_sha256",json!("00".repeat(32))),("write_control_generation",json!(10)),("write_transcript_sha256",json!("bad"))]{
  let(st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();let r=p.ranges.write.begin(a,&json!(29)).unwrap();let good=ack(&r);let mut bad=good.clone();bad[key]=value;
  assert!(p.ranges.write.finish(9,&bad).is_err(),"{key}");assert!(p.ranges.write.has_unknown());assert!(p.ranges.write.finish(9,&good).is_err());assert!(p.frozen());
 }
}
#[test]fn exact_flush_end_allows_commit_once_and_fences_generation_replay(){
 let(st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();let r=p.ranges.write.begin(a.clone(),&json!(29)).unwrap();
 assert!(p.ranges.write.begin(a.clone(),&json!(29)).is_err());assert!(p.ranges.commit(&l,"early",65536).is_err());p.ranges.write.finish(9,&ack(&r)).unwrap();
 p.ranges.leases.get_mut(&l.token).unwrap().state="Active";assert_eq!(p.ranges.commit(&l,"actual-native-verified",65536).unwrap(),2);assert!(p.ranges.write.is_empty());assert!(p.ranges.write.begin(a,&json!(29)).is_err());
 assert_eq!(p.ranges.snapshot()["managed_write_obligations"]["outstanding"],json!([]));
}

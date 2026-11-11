use super::*;
use crate::{managed_pool::{State,Peer},pool_provider::pidfd,ddr_resources::BackingId};

pub(crate) fn fixture()->(Catalog,State,RangeLease,Value){
 let mut st=Catalog::new(-1,Vec::new());let mut state=State::default();
 state.create_pending(&mut st,"pool".into(),"epoch".into(),"provider".into(),131072,"create").unwrap();
 let p=state.pools.get_mut("pool").unwrap();p.state="Allocated";p.backend="mem_service_obmm";p.canonical=json!({"generation":1});
 let rb=st.resource_bindings.get_mut(&p.binding).unwrap();rb.provider_resource_id="pool".into();rb.provider_session_id="provider".into();rb.binding_generation=1;
 st.ddr_resources.commit_backing("create",p.object,BackingId::Provider{realm:"realm".into(),provider:"mem_service_obmm".into(),resource:"nodeA:7:pool".into(),generation:1},131072).unwrap();
 let pid=std::process::id() as i32;
 let peer=Peer{birth:crate::ddr_process_identity::observe(pid,unsafe{libc::geteuid()},unsafe{libc::getegid()}).unwrap(),pidfd:pidfd(pid).unwrap()};
 let r=state.metadata(&mut st,&peer,&json!({"op":"RegisterPoolClient","pool_id":"pool","pool_epoch":"epoch","access":"rw"})).unwrap();
 let client=r["client_session"].as_str().unwrap().to_owned();let access=r["access_generation"].as_u64().unwrap();
 state.pools.get_mut("pool").unwrap().clients.get_mut(&client).unwrap().state="Mapped";
 let binding=st.access_bindings.get_mut(&access).unwrap();binding.state="Mapped".into();binding.accessor_node_id="nodeB".into();
 let base=json!({"pool_id":"pool","pool_epoch":"epoch","provider_session":"provider","client_session":client,"access_generation":access});
 let mut q=base.clone();q["op"]=json!("AllocatePoolRange");q["nbytes"]=json!(131072);q["alignment"]=json!(65536);
 let block=state.metadata(&mut st,&peer,&q).unwrap();
 let mut q=base;q["op"]=json!("AcquirePoolRange");q["block"]=block;q["mode"]=json!("w");q["expected_generation"]=json!(1);q["byte_offset"]=json!(0);q["nbytes"]=json!(65536);
 let wr=state.metadata(&mut st,&peer,&q).unwrap();
 let p=state.pools.get_mut("pool").unwrap();let w=p.ranges.leases[wr["lease_id"].as_str().unwrap()].clone();p.ranges.commit(&w,"committed-content",65536).unwrap();st.leases.get_mut(&w.catalog_id).unwrap().state="Closed".into();
 q["mode"]=json!("r");let rd=state.metadata(&mut st,&peer,&q).unwrap();
 let p=state.pools.get_mut("pool").unwrap();let lease=p.ranges.leases.get_mut(rd["lease_id"].as_str().unwrap()).unwrap();lease.state="Executing";let lease=lease.clone();
 (st,state,lease,json!({"node":"nodeB","boot_id":"boot-b","agent_session":"session-b","incarnation":8}))
}
fn grant(st:&Catalog,state:&State,l:&RangeLease,node:&Value,g:u64)->Authority{Authority::mint(&state.pools["pool"],st,l,node,"realm","actual-operation",g).unwrap()}
fn ack(r:&Value)->Value{json!({"cache_version":1,"cache_control_generation":r["cache_control_generation"],"cache_authority_sha256":r["cache_authority_sha256"],"cache_frame_sha256":r["cache_frame_sha256"],"cache_begin_confirmed":true,"cache_end_confirmed":true,"cache_active":false,"cache_in_flight":0,"cache_pages":0,"cache_transcript_sha256":"ab".repeat(32)})}

#[test]fn real_server_state_mints_bounded_full_authority(){
 let(st,state,l,node)=fixture();let r=grant(&st,&state,&l,&node,4294967296).request(&json!(17)).unwrap();
 assert!(serde_json::to_vec(&r).unwrap().len()<=1800);assert_eq!(r["cache_lease_generation"],l.catalog_id);assert_eq!(r["cache_block_generation"],l.generation);assert_eq!(r["cache_backing_resource"],"nodeA:7:pool");
 assert_ne!(r["cache_frame_sha256"],grant(&st,&state,&l,&node,4294967297).request(&json!(17)).unwrap()["cache_frame_sha256"]);
}
#[test]fn stale_cross_client_unexecuted_and_wrong_native_backing_cannot_mint(){
 let(mut st,mut state,l,node)=fixture();
 for key in ["client","version","token","offset"]{let mut bad=l.clone();match key{"client"=>bad.client="other".into(),"version"=>bad.version+=1,"token"=>bad.token="old".into(),_=>bad.offset+=65536};assert!(Authority::mint(&state.pools["pool"],&st,&bad,&node,"realm","op",1).is_err());}
 let mut bad=node.clone();bad["node"]=json!("nodeA");assert!(Authority::mint(&state.pools["pool"],&st,&l,&bad,"realm","op",1).is_err());
 state.pools.get_mut("pool").unwrap().ranges.leases.get_mut(&l.token).unwrap().state="Active";assert!(Authority::mint(&state.pools["pool"],&st,&l,&node,"realm","op",1).is_err());
 state.pools.get_mut("pool").unwrap().ranges.leases.get_mut(&l.token).unwrap().state="Executing";
 st.resource_bindings.get_mut(&state.pools["pool"].binding).unwrap().binding_generation+=1;assert!(Authority::mint(&state.pools["pool"],&st,&l,&node,"realm","op",1).is_err());
}
#[test]fn only_exact_end_ack_removes_real_obligation_and_replay_stays_fenced(){
 let(st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();let r=p.ranges.cache.begin(a.clone(),&json!(17)).unwrap();
 assert!(p.ranges.cache.begin(a.clone(),&json!(17)).is_err());assert!(p.ranges.cache.guard_writer(0,65536).is_err());assert!(p.ranges.cache.guard_writer(65536,65536).is_ok());
 p.ranges.cache.finish(9,&ack(&r)).unwrap();assert!(p.ranges.cache.is_empty());assert!(p.ranges.cache.begin(a,&json!(17)).is_err());assert!(p.ranges.cache.guard_writer(0,65536).is_ok());
 assert_eq!(p.ranges.snapshot()["managed_cache_obligations"]["control_highwater"],9);assert_eq!(p.ranges.snapshot()["managed_cache_obligations"]["outstanding"],json!([]));
}
#[test]fn dropped_forged_partial_or_wrong_typed_ack_freezes_even_after_late_ack(){
 for(key,value)in [("cache_end_confirmed",json!(false)),("cache_authority_sha256",json!("00".repeat(32))),("cache_frame_sha256",json!("00".repeat(32))),("cache_control_generation",json!(10)),("cache_active",json!(true)),("cache_in_flight",json!(false)),("cache_pages",json!(0.0)),("cache_transcript_sha256",json!("bad"))]{
  let(st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();let r=p.ranges.cache.begin(a,&json!(17)).unwrap();let good=ack(&r);let mut bad=good.clone();bad[key]=value;
  assert!(p.ranges.cache.finish(9,&bad).is_err(),"{key}");assert!(p.ranges.cache.has_unknown());assert!(p.frozen());assert!(p.ranges.cache.finish(9,&good).is_err());assert!(p.ranges.cache.guard_writer(0,65536).is_err());
 }
}
#[test]fn independent_cache_obligation_blocks_bookkeeping_close_unmap_release_and_writer(){
 let(mut st,mut state,l,node)=fixture();let a=grant(&st,&state,&l,&node,9);let p=state.pools.get_mut("pool").unwrap();p.ranges.cache.begin(a,&json!(17)).unwrap();
 p.ranges.leases.get_mut(&l.token).unwrap().state="Closed";p.clients.get_mut(&l.client).unwrap().closing=true;
 assert!(p.ranges.release(&l).is_err());p.ranges.reconcile_unmapped(&l.client);assert!(p.confirm_client_unmapped(&mut st,&l.client).is_err());assert!(p.confirm_closed(&st).is_err());
 assert!(p.ranges.acquire("writer".into(),999,l.client.clone(),l.block,l.generation,0,65536,"w").is_err());
 let mut w=l.clone();w.mode="w".into();assert!(p.ranges.commit(&w,"changed",65536).is_err());
 p.ranges.cache.uncertain(9);assert!(p.ranges.cache.has_unknown());assert_eq!(st.ddr_resources.charged_bytes(),131072);
}

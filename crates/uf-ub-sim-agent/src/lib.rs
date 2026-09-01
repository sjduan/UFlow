use std::collections::BTreeMap;
use uf_core::bridge::{
    BridgeFrame, BridgeMessageType, BridgeProtocolError, ReplayCache, ReplayDecision,
};

pub struct MockCompanion {
    replay: ReplayCache,
    counters: BTreeMap<String, u64>,
}

impl Default for MockCompanion {
    fn default() -> Self {
        Self {
            replay: ReplayCache::default(),
            counters: BTreeMap::new(),
        }
    }
}

impl MockCompanion {
    pub fn handle(&mut self, request: BridgeFrame) -> Result<BridgeFrame, BridgeProtocolError> {
        match self.replay.observe(&request) {
            ReplayDecision::Cached(encoded) => return BridgeFrame::decode(&encoded),
            ReplayDecision::Conflict => {
                let mut response = self.response(&request, BridgeMessageType::Error);
                response.status = -1;
                response.error_code = 1006;
                response.body = b"ProviderProtocolError: request_id replay conflict".to_vec();
                return Ok(response);
            }
            ReplayDecision::New => {}
        }
        *self
            .counters
            .entry(format!("request.{}", request.message_type as u16))
            .or_insert(0) += 1;
        let response_type = match request.message_type {
            BridgeMessageType::Identity
            | BridgeMessageType::Probe
            | BridgeMessageType::Shutdown => BridgeMessageType::Completion,
            _ => BridgeMessageType::Accepted,
        };
        let mut response = self.response(&request, response_type);
        response.body = match request.message_type {
            BridgeMessageType::Identity => b"provider=mock-obmm;api=1;abi=1".to_vec(),
            BridgeMessageType::Probe => b"health=ready;fidelity=semantic".to_vec(),
            BridgeMessageType::Shutdown => b"shutdown=accepted".to_vec(),
            _ => format!(
                "terminal=complete;operation={}",
                request.message_type as u16
            )
            .into_bytes(),
        };
        self.replay
            .remember(&request, response.encode().expect("valid mock response"));
        Ok(response)
    }

    fn response(&self, request: &BridgeFrame, message_type: BridgeMessageType) -> BridgeFrame {
        let mut response = BridgeFrame::request(
            message_type,
            request.request_id,
            request.correlation_id,
            request.execution_realm_id.clone(),
            request.node_id.clone(),
            request.boot_id.clone(),
            request.provider_session_id.clone(),
            request.session_generation,
            Vec::new(),
        );
        response.flags = 1;
        response
    }

    pub fn counters(&self) -> BTreeMap<String, u64> {
        self.counters.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: u64, body: &[u8]) -> BridgeFrame {
        BridgeFrame::request(
            BridgeMessageType::Probe,
            id,
            id,
            "realm",
            "nodeA",
            "boot",
            "session",
            1,
            body.to_vec(),
        )
    }

    #[test]
    fn one_request_has_one_replayed_terminal_response() {
        let mut companion = MockCompanion::default();
        let first = companion.handle(request(1, b"probe")).unwrap();
        let replay = companion.handle(request(1, b"probe")).unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.message_type, BridgeMessageType::Completion);
        let conflict = companion.handle(request(1, b"different")).unwrap();
        assert_eq!(conflict.message_type, BridgeMessageType::Error);
        assert_eq!(companion.counters().values().sum::<u64>(), 1);
    }
}

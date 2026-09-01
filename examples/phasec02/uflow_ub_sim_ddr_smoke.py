#!/usr/bin/env python3
"""PhaseC-02 fixed 4 KiB NodeA.DDR -> NodeB read-only validation slice."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from uflow import HandleNotLocallyAddressable, UFlowClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--seed-id", type=int, default=42)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence_path = Path(args.evidence)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    client = UFlowClient(
        enabled=True,
        socket_path=args.socket,
        device_id=0,
        acl_lib_path=os.environ.get("UF_ACL_LIB", "/nonexistent/libuf_acl_shim.so"),
        client_role="phasec02-smoke",
        model_id="phasec02",
    )
    result: dict[str, object] = {
        "schema": "uflow-phasec02-golden-slice-v1",
        "run_id": args.run_id,
        "execution_mode": "simulated",
        "provider": "ub_sim",
        "fidelity": "functional",
        "nbytes": 4096,
        "offset_bytes": 0,
        "access_mode": "read",
        "source_node": "nodeA",
        "accessor_node": "nodeB",
    }
    try:
        capabilities = client.capabilities()
        if "ub_sim" not in capabilities.get("providers", "").split(","):
            raise AssertionError("ub_sim provider was not registered")

        obj = client.sim("ub_sim").create_data_object(
            name="phasec02-ddr-x",
            size=4096,
            medium="ddr",
            target={"node_id": "nodeA", "domain": "ub_domain0"},
            role="validation",
            seed_id=args.seed_id,
        )
        host_dereference_rejected = False
        try:
            _ = obj.data_ptr
        except HandleNotLocallyAddressable:
            host_dereference_rejected = True
        if not host_dereference_rejected:
            raise AssertionError("simulated handle became Host-addressable")

        published = client.publish_simulated(obj, offset_bytes=0, nbytes=4096)
        access = client.import_simulated_read_only(
            obj,
            accessor_node_id="nodeB",
            offset_bytes=0,
            nbytes=4096,
        )
        observed_checksum = access.checksum(offset_bytes=0, nbytes=4096)
        if observed_checksum != obj.seed_checksum:
            raise AssertionError(
                f"checksum mismatch: nodeA={obj.seed_checksum} nodeB={observed_checksum}"
            )
        if access.placement_count != 1 or access.remote_map_created_placement:
            raise AssertionError("remote map incorrectly created a NodeB Placement")

        validation = client.get_simulated_validation(access.plan_id)
        if validation["parent_terminal_count"] != 1:
            raise AssertionError("validation plan did not produce exactly one terminal parent")
        if len(validation["plan"]["steps"]) != 3:
            raise AssertionError("validation plan does not contain the three provider steps")

        access_binding_id = access.access_binding_id
        access.revoke()
        stale_rejected = False
        try:
            access.checksum(offset_bytes=0, nbytes=4096)
        except RuntimeError:
            stale_rejected = True
        if not stale_rejected:
            raise AssertionError("revoked AccessBinding remained readable")

        obj.release()
        trace = client.flush_trace()
        result.update(
            {
                "status": "pass",
                "object_id": obj.object_id,
                "placement_id": obj.placement_id,
                "resource_binding_id": obj.binding_id,
                "access_binding_id": access_binding_id,
                "provider_session_id_nodeA": published["provider_session_id"],
                "provider_session_id_nodeB": access.accessor_provider_session_id,
                "provider_resource_id_nodeB": access.accessor_resource_id,
                "publish_completion_id": published["provider_completion_id"],
                "import_completion_id": access.provider_completion_id,
                "checksum_completion_id": access.checksum_completion_id,
                "plan_id": access.plan_id,
                "parent_event_id": access.parent_event_id,
                "publish_step_event_id": int(published["step_event_id"]),
                "import_step_event_id": access.import_step_event_id,
                "checksum_step_event_id": access.checksum_step_event_id,
                "parent_terminal_count": access.parent_terminal_count,
                "typed_plan": validation["plan"],
                "typed_events": validation["events"],
                "monitor_snapshot": validation["monitor"],
                "placement_count": access.placement_count,
                "remote_map_created_placement": access.remote_map_created_placement,
                "nodeA_seed_checksum": obj.seed_checksum,
                "nodeB_observed_checksum": observed_checksum,
                "checksum_verdict": "pass",
                "host_dereference_rejected": host_dereference_rejected,
                "revoke_stale_rejected": stale_rejected,
                "execution_realm_id": obj.execution_realm_id,
                "addressability": obj.addressability,
                "trace_artifact_dir": trace.get("trace_output_dir", ""),
                "trace_event_count": int(trace.get("trace_event_count", "0")),
            }
        )
    except Exception as error:
        result.update({"status": "fail", "error": str(error)})
        evidence_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        raise
    finally:
        client.close()

    evidence_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

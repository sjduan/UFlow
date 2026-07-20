from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SDK_ROOT = REPO_ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from uflow import SsdObject, UFlowClient  # noqa: E402


DEFAULT_SIZES = "4MiB,16MiB,32MiB,64MiB,128MiB,256MiB,512MiB,1024MiB,2048MiB"


def parse_size(text: str) -> int:
    value = text.strip()
    lower = value.lower()
    scale = 1
    for suffix, factor in (
        ("gib", 1024**3),
        ("gb", 1024**3),
        ("mib", 1024**2),
        ("mb", 1024**2),
        ("kib", 1024),
        ("kb", 1024),
    ):
        if lower.endswith(suffix):
            scale = factor
            value = value[: -len(suffix)]
            break
    return int(value) * scale


def parse_sizes(text: str) -> list[int]:
    return [parse_size(item) for item in text.split(",") if item.strip()]


def client(*, socket_path: str, device: int, role: str, model_id: str = "phaseb01") -> UFlowClient:
    return UFlowClient(
        enabled=True,
        socket_path=socket_path,
        device_id=device,
        acl_lib_path=os.environ.get("UF_ACL_LIB", "/home/sj/git/data-service/build/lib/libuf_acl_shim.so"),
        client_role=role,
        model_id=model_id,
    )


def assert_capabilities(uf: UFlowClient) -> None:
    caps = uf.capabilities()
    placements = set(caps.get("placements", "").split(","))
    address_kinds = set(caps.get("address_kinds", "").split(","))
    if "ssd" not in placements:
        raise AssertionError(f"GetCapabilities missing ssd placement: {caps}")
    if "file_path_offset" not in address_kinds:
        raise AssertionError(f"GetCapabilities missing file_path_offset: {caps}")


def assert_stats_delta(before: dict[str, str], during: dict[str, str], after: dict[str, str], nbytes: int) -> None:
    before_count = int(before.get("ssd_objects", "0"))
    during_count = int(during.get("ssd_objects", "0"))
    after_count = int(after.get("ssd_objects", "0"))
    if during_count != before_count + 1:
        raise AssertionError(f"ssd_objects did not increase by 1: before={before_count} during={during_count}")
    if after_count != before_count:
        raise AssertionError(f"ssd_objects did not return to baseline: before={before_count} after={after_count}")
    during_actual = int(during.get("ssd_actual_bytes", "0"))
    before_actual = int(before.get("ssd_actual_bytes", "0"))
    if during_actual - before_actual < nbytes:
        raise AssertionError(
            f"ssd_actual_bytes delta too small: before={before_actual} during={during_actual} requested={nbytes}"
        )


def exercise_one(args: argparse.Namespace, nbytes: int, index: int) -> None:
    parent = client(socket_path=args.socket, device=args.device, role="phaseb01-parent")
    child = client(socket_path=args.socket, device=args.device, role="phaseb01-child")
    obj: SsdObject | None = None
    opened: SsdObject | None = None
    before = parent.stats()
    try:
        obj = parent.allocate_ssd(
            name=f"phaseb01.ssd.{index}.{nbytes}",
            role="user",
            nbytes=nbytes,
            target=args.target,
            mark_ready=False,
        )
        path = Path(obj.path)
        if not path.exists():
            raise AssertionError(f"SSD object backing file does not exist: {path}")
        if path.name.startswith("phaseb01"):
            raise AssertionError(f"SSD backing path unexpectedly includes user object name: {path}")
        if obj.actual_bytes < obj.requested_bytes:
            raise AssertionError(f"actual_bytes={obj.actual_bytes} < requested_bytes={obj.requested_bytes}")
        parent.mark_ready(obj)
        described, placement = parent.describe_object(obj.object_id)
        if described.state != "Ready":
            raise AssertionError(f"MarkReady did not set Ready, got {described.state}")
        if placement.medium != "ssd" or placement.address_kind != "file_path_offset":
            raise AssertionError(f"unexpected placement after DescribeObject: {placement}")
        parent.mark_dirty(obj, offset_bytes=0, nbytes=min(nbytes, 4096))
        described, _ = parent.describe_object(obj.object_id)
        if described.state != "Modified":
            raise AssertionError(f"MarkDirty did not set Modified, got {described.state}")
        open_offset = min(nbytes // 4, nbytes - 1)
        open_bytes = min(max(nbytes // 2, 1), nbytes - open_offset)
        opened = child.open_ssd(
            object_id=obj.object_id,
            target=args.target,
            allowed_offset_bytes=open_offset,
            allowed_bytes=open_bytes,
        )
        if opened.object_id != obj.object_id or opened.lease_id == obj.lease_id:
            raise AssertionError(f"OpenDataObject did not return a distinct SSD lease: parent={obj} child={opened}")
        if opened.offset_bytes != open_offset or opened.nbytes != open_bytes:
            raise AssertionError(
                f"OpenDataObject returned wrong range: offset={opened.offset_bytes}/{open_offset} bytes={opened.nbytes}/{open_bytes}"
            )
        during = parent.stats()
        opened.release(release_object=False)
        opened = None
        path_before_release = Path(obj.path)
        obj.release()
        obj = None
        after = parent.stats()
        if path_before_release.exists():
            raise AssertionError(f"SSD backing file was not removed after release: {path_before_release}")
        assert_stats_delta(before, during, after, nbytes)
        print(
            "UFLOW_B01_SSD_OBJECT_PASS "
            f"bytes={nbytes} object_id={described.object_id} placement_id={placement.placement_id} "
            f"actual_bytes={placement.nbytes} path={path_before_release}",
            flush=True,
        )
    finally:
        if opened is not None:
            opened.release(release_object=False)
        if obj is not None:
            obj.release()
        child.close()
        parent.close()


def expect_failure(label: str, fn) -> None:
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - smoke prints daemon error detail.
        print(f"UFLOW_B01_EXPECTED_FAILURE label={label} detail={exc}", flush=True)
        return
    raise AssertionError(f"expected failure did not happen: {label}")


def negative_cases(args: argparse.Namespace) -> None:
    uf = client(socket_path=args.socket, device=args.device, role="phaseb01-negative")
    obj: SsdObject | None = None
    try:
        expect_failure(
            "invalid_target",
            lambda: uf.allocate_ssd(name="phaseb01.invalid_target", nbytes=4096, target="ssd:remote0"),
        )
        obj = uf.allocate_ssd(name="phaseb01.range", nbytes=4096, target=args.target)
        expect_failure(
            "open_range_out_of_bounds",
            lambda: uf.open_ssd(object_id=obj.object_id, target=args.target, allowed_offset_bytes=4096, allowed_bytes=1),
        )
    finally:
        if obj is not None:
            obj.release()
        uf.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default=os.environ.get("UF_SOCKET", "/tmp/uflow.sock"))
    parser.add_argument("--device", type=int, default=int(os.environ.get("UF_TARGET_DEVICE", "0")))
    parser.add_argument("--target", default=os.environ.get("UF_SSD_TARGET", "ssd:local0"))
    parser.add_argument("--sizes", default=DEFAULT_SIZES)
    parser.add_argument("--skip-negative", action="store_true")
    args = parser.parse_args()

    uf = client(socket_path=args.socket, device=args.device, role="phaseb01-capabilities")
    try:
        assert_capabilities(uf)
        print(f"UFLOW_B01_CAPABILITIES_PASS socket={args.socket}", flush=True)
    finally:
        uf.close()

    for index, nbytes in enumerate(parse_sizes(args.sizes)):
        exercise_one(args, nbytes, index)
    if not args.skip_negative:
        negative_cases(args)
    print("UFLOW_B01_ALL_PASS", flush=True)


if __name__ == "__main__":
    main()

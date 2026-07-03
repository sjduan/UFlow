from __future__ import annotations

import threading
from dataclasses import dataclass

from .client import UFlowClient
from .idl import TransferEvent, TransferPlan
from .objects import DdrBuffer, ManagedBuffer
from .transfer import AclEventHandle, TransferCompletionEventHandle


@dataclass(frozen=True)
class LayerWeightPlacement:
    layer_idx: int
    ddr: DdrBuffer
    hbm: ManagedBuffer
    nbytes: int
    name: str = ""
    immutable: bool = True


@dataclass
class LayerReloadState:
    layer_idx: int
    plan: TransferPlan
    transfer_event: TransferEvent
    completion_event: TransferCompletionEventHandle


class AsyncLayerWeightPipeline:
    """PhaseA-08 helper for event-driven layer weight reload/evict orchestration.

    Official data movement goes through UFlow daemon ``PlanTransfer`` and
    ``SubmitTransfer`` via ``submit_transfer_hotpath_event``. The current
    completion export may still be host-assisted, but the DDR/HBM copy itself is
    always daemon-side for the PhaseA-08 serving contract.
    """

    def __init__(
        self,
        client: UFlowClient,
        layers: list[LayerWeightPlacement],
        *,
        transfer_mode: str = "auto",
        timeout_ms: int = 120_000,
    ) -> None:
        self.client = client
        self.layers = {int(layer.layer_idx): layer for layer in layers}
        self.transfer_mode = transfer_mode
        self.timeout_ms = int(timeout_ms)
        self._reloads: dict[int, LayerReloadState] = {}
        self._evict_threads: dict[int, threading.Thread] = {}
        self._evict_errors: dict[int, BaseException] = {}

    def submit_all_reloads(self) -> None:
        for layer_idx in sorted(self.layers):
            self.submit_reload(layer_idx)

    def submit_reload(self, layer_idx: int) -> LayerReloadState:
        if layer_idx in self._reloads:
            return self._reloads[layer_idx]
        layer = self.layers[layer_idx]
        plan = self.client.plan_transfer(
            src=layer.ddr,
            dst=layer.hbm,
            nbytes=layer.nbytes,
            mode=self.transfer_mode,
            wait_policy="return_immediately",
        )
        completion = self.client.submit_transfer_hotpath_event(
            src=layer.ddr,
            dst=layer.hbm,
            nbytes=layer.nbytes,
            mode=self.transfer_mode,
            timeout_ms=self.timeout_ms,
            plan=plan,
        )
        event = self.client.poll_event(completion.transfer_event_id)
        state = LayerReloadState(
            layer_idx=layer_idx,
            plan=plan,
            transfer_event=event,
            completion_event=completion,
        )
        self._reloads[layer_idx] = state
        return state

    def reload_event(self, layer_idx: int) -> TransferCompletionEventHandle:
        return self.submit_reload(layer_idx).completion_event

    def wait_reload_proxy(self, layer_idx: int) -> None:
        event = self.reload_event(layer_idx)
        self.client.wait_completion_event_proxy(event, timeout_s=self.timeout_ms / 1000.0)

    def submit_layer_evict_after_event(
        self,
        layer_idx: int,
        compute_done_event: AclEventHandle | TransferCompletionEventHandle | int,
        *,
        dirty: bool = False,
        release_hbm: bool = True,
    ) -> None:
        if layer_idx not in self.layers:
            raise KeyError(layer_idx)
        if layer_idx in self._evict_threads:
            raise RuntimeError(f"layer {layer_idx} evict is already submitted")
        layer = self.layers[layer_idx]

        def _evict() -> None:
            try:
                self.client.synchronize_event_handle(compute_done_event)
                if dirty:
                    plan = self.client.plan_transfer(
                        src=layer.hbm,
                        dst=layer.ddr,
                        nbytes=layer.nbytes,
                        mode=self.transfer_mode,
                        wait_policy="return_immediately",
                    )
                    event = self.client.submit_transfer(plan)
                    self.client.wait_event(event, timeout_ms=self.timeout_ms)
                if release_hbm:
                    layer.hbm.release()
            except BaseException as exc:  # noqa: BLE001 - re-raised by wait_all_evicts.
                self._evict_errors[layer_idx] = exc

        thread = threading.Thread(
            target=_evict,
            name=f"uflow-layer-evict-{layer_idx}",
            daemon=True,
        )
        self._evict_threads[layer_idx] = thread
        thread.start()

    def wait_all_reloads(self) -> None:
        for layer_idx in sorted(self.layers):
            self.wait_reload_proxy(layer_idx)

    def wait_all_evicts(self) -> None:
        for layer_idx, thread in list(self._evict_threads.items()):
            thread.join(timeout=self.timeout_ms / 1000.0)
            if thread.is_alive():
                raise TimeoutError(f"timed out waiting for layer {layer_idx} evict thread")
            self._evict_threads.pop(layer_idx, None)
        if self._evict_errors:
            layer_idx, error = next(iter(self._evict_errors.items()))
            self._evict_errors.clear()
            raise RuntimeError(f"layer {layer_idx} evict failed") from error

    def close(self) -> None:
        self.wait_all_evicts()
        for state in list(self._reloads.values()):
            self.client.wait_completion_event_proxy(
                state.completion_event,
                timeout_s=self.timeout_ms / 1000.0,
                synchronize_acl_event=False,
            )
            self.client.destroy_event_handle(state.completion_event)
        self._reloads.clear()


class DecodeWeightResidency:
    """Keep decode weights resident for a fused decode graph, then release them."""

    def __init__(
        self,
        client: UFlowClient,
        weights: list[LayerWeightPlacement],
        *,
        transfer_mode: str = "auto",
        timeout_ms: int = 120_000,
    ) -> None:
        self._pipeline = AsyncLayerWeightPipeline(
            client,
            weights,
            transfer_mode=transfer_mode,
            timeout_ms=timeout_ms,
        )

    def load_all(self) -> list[TransferCompletionEventHandle]:
        self._pipeline.submit_all_reloads()
        return [self._pipeline.reload_event(layer_idx) for layer_idx in sorted(self._pipeline.layers)]

    def wait_loaded(self) -> None:
        self._pipeline.wait_all_reloads()

    def release_after_decode(self, decode_done_event: AclEventHandle | TransferCompletionEventHandle | int) -> None:
        for layer_idx in sorted(self._pipeline.layers):
            self._pipeline.submit_layer_evict_after_event(layer_idx, decode_done_event, dirty=False, release_hbm=True)

    def close(self) -> None:
        self._pipeline.close()

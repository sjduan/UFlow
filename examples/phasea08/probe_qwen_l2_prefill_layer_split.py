from __future__ import annotations

import argparse
import ast
import json
import os
import re
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]


def _default_pypto_serving_root() -> Path:
    candidates = [
        REPO_ROOT / "pypto-serving",
        REPO_ROOT / "worktrees" / "pypto-serving-phasea07-ddr",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return candidates[0]


def _read(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def _function_names(source: str) -> set[str]:
    tree = ast.parse(source)
    return {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _has_decorator(source: str, func_name: str, pattern: str) -> bool:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            return any(pattern in ast.unparse(dec) for dec in node.decorator_list)
    return False


def _find_qwen14b_dir(pypto_serving_root: Path) -> Path:
    candidates = [
        pypto_serving_root / "pypto-lib" / "models" / "qwen3" / "14b",
        REPO_ROOT / "pypto-lib" / "models" / "qwen3" / "14b",
    ]
    for candidate in candidates:
        if (candidate / "prefill_fwd.py").is_file():
            return candidate
    raise FileNotFoundError("Cannot locate Qwen3-14B pypto-lib prefill_fwd.py")


def _status_line(key: str, value: Any) -> str:
    if isinstance(value, bool):
        value = str(value).lower()
    return f"{key}={value}"


def probe(pypto_serving_root: Path) -> dict[str, Any]:
    qwen_dir = _find_qwen14b_dir(pypto_serving_root)
    prefill_path = qwen_dir / "prefill_fwd.py"
    decode_path = qwen_dir / "decode_layer.py"
    runner_path = pypto_serving_root / "examples" / "model" / "qwen3_14b" / "runner" / "npu_runner.py"
    executor_path = pypto_serving_root / "examples" / "model" / "qwen3_14b" / "runner" / "npu_executor.py"

    prefill_src = _read(prefill_path)
    decode_src = _read(decode_path) if decode_path.is_file() else ""
    runner_src = _read(runner_path) if runner_path.is_file() else ""
    executor_src = _read(executor_path) if executor_path.is_file() else ""

    prefill_funcs = _function_names(prefill_src)
    decode_funcs = _function_names(decode_src) if decode_src else set()

    has_inline_layer = "prefill_layer" in prefill_funcs and _has_decorator(prefill_src, "prefill_layer", "pl.jit.inline")
    has_top_prefill = "prefill_fwd" in prefill_funcs and _has_decorator(prefill_src, "prefill_fwd", "pl.jit")
    has_top_layer_wrapper = "prefill_layer_fwd" in prefill_funcs and _has_decorator(prefill_src, "prefill_layer_fwd", "pl.jit")
    has_prefill_finalize = "prefill_finalize_fwd" in prefill_funcs and _has_decorator(prefill_src, "prefill_finalize_fwd", "pl.jit")
    all_layer_loop = bool(re.search(r"for\s+layer_idx\s+in\s+pl\.range\(num_layers_actual\)", prefill_src))
    runner_single_prefill = "compiled.prefill," in runner_src and "fused" in runner_src
    executor_single_compile = "_compile_prefill_fwd_callable" in executor_src and "prefill_layer_fwd" not in executor_src
    decode_whole_graph = "decode_fwd" in decode_funcs and "run_decode" in runner_src and "fused all-layer" in runner_src
    decode_has_chunk_candidate = "decode_fwd_layers" in decode_funcs

    algorithmic_boundary_exists = has_inline_layer and all_layer_loop
    top_level_layer_task_ready = has_top_layer_wrapper and has_prefill_finalize and not executor_single_compile
    runner_layer_pipeline_ready = "run_prefill_layer" in runner_src or "_run_prefill_layer" in runner_src
    supported = top_level_layer_task_ready and runner_layer_pipeline_ready

    blockers: list[str] = []
    required_changes: list[str] = []
    if not has_top_layer_wrapper:
        blockers.append("missing top-level @pl.jit prefill_layer_fwd wrapper")
        required_changes.append("add prefill_layer_fwd(...) that calls existing @pl.jit.inline prefill_layer(...) for one layer")
    if not has_prefill_finalize:
        blockers.append("missing top-level @pl.jit prefill_finalize_fwd wrapper")
        required_changes.append("add prefill_finalize_fwd(...) for gather-last-token + final_norm + lm_head")
    if executor_single_compile:
        blockers.append("npu_executor only compiles fused prefill_fwd")
        required_changes.append("compile per-layer prefill callable and finalizer callable")
    if not runner_layer_pipeline_ready:
        blockers.append("npu_runner only dispatches compiled.prefill once")
        required_changes.append("add runner path that dispatches 40 prefill layer tasks and then finalizer")

    if not algorithmic_boundary_exists:
        blockers.append("prefill algorithm does not expose an inline layer boundary")
        required_changes.append("split prefill_fwd loop body into a reusable per-layer function")

    result = {
        "pypto_serving_root": str(pypto_serving_root),
        "qwen_dir": str(qwen_dir),
        "prefill_path": str(prefill_path),
        "runner_path": str(runner_path),
        "executor_path": str(executor_path),
        "algorithmic_boundary_exists": algorithmic_boundary_exists,
        "has_inline_prefill_layer": has_inline_layer,
        "has_top_prefill_fwd": has_top_prefill,
        "has_top_prefill_layer_fwd": has_top_layer_wrapper,
        "has_top_prefill_finalize_fwd": has_prefill_finalize,
        "prefill_fwd_has_all_layer_loop": all_layer_loop,
        "runner_currently_single_fused_prefill": runner_single_prefill,
        "executor_currently_single_fused_compile": executor_single_compile,
        "decode_policy_whole_graph_ok": decode_whole_graph,
        "decode_has_chunk_candidate_but_not_used": decode_has_chunk_candidate,
        "prefill_layer_split_supported": supported,
        "blockers": blockers,
        "required_changes": required_changes,
    }
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe whether Qwen3-14B L2 prefill can currently be split into layer tasks.")
    parser.add_argument("--pypto-serving-root", type=Path, default=Path(os.environ.get("PYPTO_SERVING_ROOT", _default_pypto_serving_root())))
    parser.add_argument("--json", action="store_true", help="print JSON only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = probe(args.pypto_serving_root.resolve())
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    print(_status_line("PREFILL_LAYER_SPLIT_SUPPORTED", result["prefill_layer_split_supported"]))
    print(_status_line("ALGORITHMIC_LAYER_BOUNDARY_EXISTS", result["algorithmic_boundary_exists"]))
    print(_status_line("DECODE_POLICY_WHOLE_GRAPH_OK", result["decode_policy_whole_graph_ok"]))
    print(_status_line("PREFILL_PATH", result["prefill_path"]))
    print(_status_line("RUNNER_PATH", result["runner_path"]))
    print(_status_line("EXECUTOR_PATH", result["executor_path"]))
    for blocker in result["blockers"]:
        print(f"BLOCKER={blocker}")
    for change in result["required_changes"]:
        print(f"REQUIRED_CHANGE={change}")


if __name__ == "__main__":
    main()

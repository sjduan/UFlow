#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");

const app = require(path.resolve(__dirname, "../../ui/uflow-monitor/app.js"));

test("v1 stats with missing Monitor v2 fields render as N/A", () => {
  const nodes = new Map();
  global.document = {
    activeElement: null,
    getElementById(id) {
      if (!nodes.has(id)) {
        nodes.set(id, {
          removeAttribute(name) {
            delete this[name];
          },
          textContent: "",
          title: "",
        });
      }
      return nodes.get(id);
    },
  };

  app.renderStats({ stats: { ddr_objects: 2, ddr_committed_bytes: 1024 } });

  assert.equal(nodes.get("ddr-objects").textContent, "2");
  assert.equal(nodes.get("ddr-committed").textContent, "1.00 KiB");
  assert.equal(nodes.get("hbm-objects").textContent, "N/A");
  assert.equal(nodes.get("ssd-objects").textContent, "N/A");
  assert.equal(nodes.get("executor-workers").textContent, "N/A");
  assert.equal(nodes.get("ssd-read").textContent, "N/A / N/A ops");

  delete global.document;
});

test("logical direct is never labeled physical direct", () => {
  const transfer = {
    actual_path: "ssd_to_hbm_direct",
    direct_kind: "logical",
    direct_candidate: "file_mmap_acl_direct",
  };
  assert.equal(app.actualPathLabel(transfer), "SSD↔HBM logical direct");
  assert.deepEqual(app.transferDecision(transfer), {
    className: "logical",
    detail: "file_mmap_acl_direct",
    label: "logical direct",
  });
});

test("runtime fallback and metadata mismatch remain distinct", () => {
  assert.deepEqual(
    app.transferDecision({
      planned_path: "ssd_to_hbm_direct",
      actual_path: "ssd_to_hbm_via_ddr",
      fallback_used: true,
      fallback_reason: "direct failed",
    }),
    { className: "fallback", detail: "direct failed", label: "runtime fallback" },
  );
  assert.deepEqual(
    app.transferDecision({
      planned_path: "ddr_to_hbm",
      actual_path: "unexpected_path",
      fallback_used: false,
    }),
    {
      className: "mismatch",
      detail: "ddr_to_hbm → unexpected_path",
      label: "planner/metadata mismatch",
    },
  );
});

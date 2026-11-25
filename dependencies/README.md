# Optional simulator source lock

The A5 UFlow daemon builds from the Cargo lock and the selected host's CANN toolkit. Its host checks use an ACL fence and require no Serving, PyPTO, Simpler, or PTOAS checkout.

`locked-sources.json` retains the ub_sim and QEMU overlay identities used for explicit simulator development, as well as the kernel and mem_service source references. To inspect or reapply an overlay, use `python3 tools/apply_locked_source.py ub_sim PATH --check` or select `qemu`. The original published `main` contains the full historical environment and dependency closure for replaying the prior simulator and A3 campaigns.

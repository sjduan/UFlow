# A5 supernode data management

An application or a future Serving data manager describes each object once and records its residency in HBM, DDR, or SSD through placements. Resource bindings identify actual backings and their generation. Access bindings and leases restrict what a consumer can access. Transfer completion and visibility come from the provider that executes the operation. An uncertain result retains its resource obligation.

This branch retains the UFlow IDL, provider interfaces, Catalog, local ACL/DDR/SSD backends, external DDR broker, managed range accounting, transfer execution, Python SDK and local monitor. The provider registry enables simulated providers when explicitly requested. Simulator recovery remains an opt-in implementation with dedicated fixtures.

The former A3 Simpler POSIX owner, its v1 SDK adapter, A3 launchers and evidence, older A3 dependency overlays, and the B-machine build and monitor defaults live on their original branches. The A5 CMake entry builds the native ACL shim, while host CI exercises a fenced daemon build and the shared contracts.

| Capability | Evidence and scope |
| --- | --- |
| A5 HIXL device transfer | A separate HIXL 9.3.0 probe transferred 64 KiB between two machines' HBM, using synchronous write and readback with `DEV_NET / UB_CTP`. Both endpoints independently verified the bytes and destination guards. |
| UFlow local providers | The branch includes the existing local HBM, DDR and SSD realizations. The host workflow checks shared object, transfer, budget, range, and provider contracts. |
| Cross-machine DDR and SSD | Provider implementation and physical validation are pending. |
| Serving integration | UFlow and the PR #362 Serving checkout are independent. The Serving PR's cross-DP DDR cache is scoped to its own implementation. UFlow-driven Serving, PD, DDR and SSD flows have a separate integration and validation step. |

The A5 HIXL probe exited normally on both machines. Its evidence applies to the small synchronous HBM transfer. The probe did not exercise UFlow or Serving.

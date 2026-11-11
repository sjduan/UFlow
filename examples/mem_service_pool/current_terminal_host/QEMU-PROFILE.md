Corrected QEMU is an independent profile from the unchanged kernel profile.
The original binary/path/SHA56fa and historical c064 capture remain their own
branch. A requested qemu_profile never falls back to that branch.

No corrected E release has been accepted or bound: the production trust anchor
is None. A requested profile fails before member/source execution and before
the lower service, original authority, token or Guest. The current-c064 v2
terminal production entry also remains independently not-ready.

The proposed config selection contains profile_id plus exact manifest and
validator references. References contain path, bytes and SHA256. These are
compared against a separately fixed accepted anchor, never trusted on their
own. The future anchor must bind the actual E manifest, binary, validator,
private dependency modules, and patch/source/object/link closure references.
Manager acceptance of the actual E bytes and final field API precedes adding
that anchor. No fixture hash is a production pin.

The consumer hashes all accepted code/dependency/member bytes before its first
source execution, creates fresh private modules instead of using cached E
modules, invokes validate(manifest_path, external_sha, qemu_binary_path), checks
the complete identity/closure and HOST-only/no-boot-authority output, then
rechecks bound bytes. It returns an immutable internal identity with provenance.
Private HOST fixtures have a distinct seal and cannot enter production capture.

The collector takes this validated identity and independently rechecks the
exact argv executable/path and actual /proc/exe bytes, parent/birth/UID/
namespace/argv/FD/VMA metadata. Environment values remain individually selected
source-derived inference. No RAM, transport, mapped data or process-FD contents
are read. An actual future Guest still additionally requires valid current-c064
responsibility reconciliation under the same original authority.

# Create a Snapshot from a Running or Paused Sandbox

Capture the current state of a running sandbox, or publish the artifacts that a
paused sandbox already wrote in its last pause:

```bash
aenv snapshot create <sandbox-id>
aenv snapshot create <sandbox-id> --name my-checkpoint
```

| Argument or option | Default | Description |
| --- | --- | --- |
| `<sandbox-id>` | Required | ID of the running or paused sandbox to capture. |
| `--name <name>` | None | Assigns a human-readable alias. If omitted, use the generated snapshot ID returned by the command. |

A **running** source continues running after the snapshot is created. A
**paused** source is published from its existing pause artifacts: the VM is
never resumed and its memory is not reconverted, the sandbox stays paused, and
only layers the repository does not already hold are uploaded. The creation
response carries a `publishReceipt` with the bytes uploaded, the duration, the
layers uploaded and reused, and the digest of the newest memory layer.

Continue with [Use a Snapshot](./use.md) to restore it or publish its rootfs as
an OCI image.

You can then pass either the snapshot ID or its alias to `aenv start`:

```bash
aenv start my-checkpoint
# Or:
aenv start <snapshot-id>
```

This creates a separate sandbox with a new sandbox ID. It inherits the captured
filesystem, running processes, memory state, environment variables, runtime
configuration, and resource settings.

# BranchFS

BranchFS is a FUSE-based filesystem that enables speculative branching on top of any existing filesystem. It gives AI agents isolated workspaces with instant copy-on-write branching, atomic commit-to-root, and zero-cost abort, no root privileges required.

## Features

| Feature | Description |
|---------|-------------|
| Fast Branch Creation | O(1) branch creation with copy-on-write semantics |
| Commit to Root | Changes apply directly to the base filesystem |
| Atomic Abort | Instantly invalidates branch, sibling branches unaffected |
| Atomic Commit | Applies changes and invalidates all branches atomically |
| mmap Invalidation | Memory-mapped files trigger SIGBUS after commit/abort |
| @branch Virtual Paths | Access any branch directly via `/@branch-name/` without switching |
| Portable | Works on any underlying filesystem (ext4, xfs, nfs, etc.) |

## Architecture

BranchFS is a FUSE-based filesystem that requires no root privileges. It implements file-level copy-on-write: when a file is modified on a branch, the entire file is lazily copied to the branch's delta storage, while unmodified files are resolved by walking up the branch chain to the base directory. Deletions are tracked via tombstone markers. On commit, all changes from the branch chain are applied atomically to the base directory; on abort, the branch's delta storage is simply discarded.

### Why not overlayfs?

Overlayfs only supports a single upper layer (no nested branches), and lacks commit-to-root semantics—changes remain in the upper layer rather than being applied back to the base. It also has no cross-mount cache invalidation needed for speculative execution workflows.

### Why not btrfs subvolumes?

Btrfs subvolumes are tied to the btrfs filesystem, making them non-portable across ext4, xfs, or network filesystems. Snapshots create independent copies rather than branches that commit back to a parent, and there's no mechanism for automatic cache invalidation when one snapshot's changes should affect others.

### Why not dm-snapshot?

Device mapper snapshots operate at the block level, requiring a block device, so they can't work on NFS, existing FUSE mounts, or arbitrary filesystems. Merging a snapshot back to its origin is complex and destructive, and like overlayfs, dm-snapshot only supports single-level snapshots without nested branches.

### What about FUSE overhead?

FUSE adds userspace-kernel context switches per operation, which is slower than native kernel filesystems. However, for speculative execution with AI agents, the bottleneck is typically network latency (LLM API calls at 100ms-10s) and GPU compute, not file I/O. FUSE overhead is negligible in comparison.

## Prerequisites

- Linux with FUSE support
- libfuse3 development libraries
- Rust toolchain (1.70 or later)

### Installing Dependencies

**Debian/Ubuntu:**
```bash
sudo apt install libfuse3-dev pkg-config
```

**Fedora:**
```bash
sudo dnf install fuse3-devel pkg-config
```

**Arch Linux:**
```bash
sudo pacman -S fuse3 pkg-config
```

## Building

```bash
git clone https://github.com/user/branchfs.git
cd branchfs
cargo build --release
```

The binary is located at `target/release/branchfs`.

## Usage Examples

### Basic Workflow

```bash
# Mount filesystem (auto-starts daemon, starts on main branch)
branchfs mount --base ~/project /mnt/workspace

# Create a branch (auto-switches to it)
branchfs create experiment /mnt/workspace

# Work in the branch (files modified here are isolated)
cd /mnt/workspace
echo "new code" > feature.py

# List branches for this mount
branchfs list /mnt/workspace

# Commit changes to base (switches back to main, stays mounted)
branchfs commit /mnt/workspace

# Or abort to discard (switches back to main, stays mounted)
branchfs abort /mnt/workspace

# Unmount when done (cleans up all branches, daemon exits when last mount removed)
branchfs unmount /mnt/workspace
```

### Nested Branches

```bash
# Mount and create hierarchy
branchfs mount --base ~/project /mnt/workspace
branchfs create level1 /mnt/workspace           # auto-switches to level1
branchfs create level2 /mnt/workspace -p level1 # auto-switches to level2

# Now on level2, work in it
echo "deep change" > /mnt/workspace/file.txt

# Commit from level2 applies: level2 + level1 → base, switches to main
branchfs commit /mnt/workspace
```

### @branch Virtual Paths

Every non-main branch is accessible as a virtual directory at the mount root, without switching the current branch:

```bash
branchfs mount --base ~/project /mnt/workspace

# Create two branches
branchfs create feature-a /mnt/workspace
branchfs create feature-b /mnt/workspace

# Access both branches simultaneously via @branch paths
cat /mnt/workspace/@feature-a/file.txt
cat /mnt/workspace/@feature-b/file.txt

# Write to a specific branch without switching
echo "change" > /mnt/workspace/@feature-a/src/main.rs

# Each @branch has its own control file
echo "commit" > /mnt/workspace/@feature-a/.branchfs_ctl
echo "abort" > /mnt/workspace/@feature-b/.branchfs_ctl
```

Nested branches can be accessed at both `/@child/` and `/@parent/@child/`:

```bash
branchfs create child /mnt/workspace -p feature-a

# Both paths reach the same branch
cat /mnt/workspace/@child/file.txt
cat /mnt/workspace/@feature-a/@child/file.txt
```

This is useful for multi-agent workflows where each agent can bind-mount a different `@branch` path to work on isolated branches in parallel within the same mount.

### Parallel Speculation (Multiple Mount Points)

Each mount has its own isolated branch namespace:

```bash
# Mount two isolated workspaces from the same base
branchfs mount --base ~/project /mnt/approach-a
branchfs mount --base ~/project /mnt/approach-b

# Create branches in each (isolated from each other)
branchfs create experiment /mnt/approach-a
branchfs create experiment /mnt/approach-b  # same name, different mount = OK

# Work in parallel...
echo "approach a" > /mnt/approach-a/solution.py
echo "approach b" > /mnt/approach-b/solution.py

# Commit one approach
branchfs commit /mnt/approach-a

# approach-b is unaffected (isolated mount)
cat /mnt/approach-b/solution.py  # still works
```

## Semantics

### Per-Mount Isolation

Each mount point has its own isolated branch namespace. Branches created in one mount are not visible to other mounts, even if they share the same base directory. This includes `@branch` virtual paths — `/@feature-a` on one mount has no relation to `/@feature-a` on another mount, even if they share the same base. This enables true parallel speculation without interference.

### Commit

Committing a branch applies the entire chain of changes to the base filesystem:

1. Changes are collected from the current branch up through its ancestors
2. Deletions are applied first, then file modifications
3. Mount's epoch increments, invalidating all branches in this mount
4. **Mount automatically switches to main branch** (stays mounted)
5. Memory-mapped regions trigger `SIGBUS` on next access

### Abort

Aborting discards the entire branch chain without affecting the base:

1. The entire branch chain (current branch up to main) is discarded
2. Sibling branches in the same mount continue operating normally
3. **Mount automatically switches to main branch** (stays mounted)
4. Memory-mapped regions in aborted branches trigger `SIGBUS`

### Unmount

Unmounting removes the mount and cleans up all its branches:

1. **All branches for this mount are discarded** (full cleanup)
2. Mount-specific storage is deleted
3. The daemon automatically exits when the last mount is removed
4. Other mounts are unaffected (per-mount isolation)

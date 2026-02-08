# BranchFS

BranchFS is a FUSE-based filesystem that enables speculative branching on top of any existing filesystem. It gives AI agents isolated workspaces with instant copy-on-write branching, atomic commit-to-parent, and zero-cost abort, no root privileges required.

## Features

| Feature | Description |
|---------|-------------|
| Fast Branch Creation | O(1) branch creation with copy-on-write semantics |
| Commit to Parent | Changes merge into immediate parent branch (or base if parent is main) |
| Atomic Abort | Instantly discards leaf branch, parent and siblings unaffected |
| Atomic Commit | Merges leaf branch into parent atomically |
| mmap Invalidation | Memory-mapped files trigger SIGBUS after commit/abort |
| @branch Virtual Paths | Access any branch directly via `/@branch-name/` without switching |
| Portable | Works on any underlying filesystem (ext4, xfs, nfs, etc.) |

## Architecture

BranchFS is a FUSE-based filesystem that requires no root privileges. It implements file-level copy-on-write: when a file is modified on a branch, the entire file is lazily copied to the branch's delta storage, while unmodified files are resolved by walking up the branch chain to the base directory. Deletions are tracked via tombstone markers. On commit, changes from a leaf branch are merged into its immediate parent (or applied to the base directory if the parent is main); on abort, the leaf branch's delta storage is simply discarded.

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

# List branches
branchfs list

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

# Commit from level2 merges into level1, switches to level1
branchfs commit /mnt/workspace

# Now on level1, commit to base
branchfs commit /mnt/workspace
# Changes from both level2 and level1 are now in base, switches to main
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

### Parallel Speculation (Multiple Agents)

With `@branch` virtual paths, multiple agents can work in parallel through a single mount:

```bash
# Mount once
branchfs mount --base ~/project /mnt/workspace

# Create branches for each agent
branchfs create agent-a /mnt/workspace
branchfs create agent-b /mnt/workspace

# Each agent works via its own @branch path (no switching needed)
echo "approach a" > /mnt/workspace/@agent-a/solution.py
echo "approach b" > /mnt/workspace/@agent-b/solution.py

# Commit one agent's work
echo "commit" > /mnt/workspace/@agent-a/.branchfs_ctl

# agent-b is unaffected
cat /mnt/workspace/@agent-b/solution.py  # still works
```

## Semantics

### Shared Branch Namespace

All mounts share a single branch namespace managed by the daemon. Branches created through any mount are visible from all mounts via `@branch` virtual paths. This simplifies multi-agent workflows — each agent accesses its branch via `/@branch-name/` without needing separate mount points.

### Commit

Committing merges a **leaf branch** into its immediate parent:

1. Only leaf branches can be committed, attempting to commit a branch with children returns an error
2. If the parent is **main**: tombstone deletions are applied to the base filesystem, then delta files are copied to base
3. If the parent is **another branch**: child's delta files are merged into the parent's delta directory, and tombstones are merged (child tombstones shadow parent deltas, child deltas un-tombstone parent tombstones)
4. The committed branch is removed; epoch increments
5. **Mount automatically switches to the parent branch** (stays mounted)
6. Memory-mapped regions trigger `SIGBUS` on next access

### Abort

Aborting discards only the **leaf branch** without affecting the parent:

1. Only leaf branches can be aborted, attempting to abort a branch with children returns an error
2. The leaf branch's delta storage is discarded
3. Other branches (including the parent) continue operating normally
4. **Mount automatically switches to the parent branch** (stays mounted)
5. Memory-mapped regions in the aborted branch trigger `SIGBUS`

### Unmount

Unmounting removes the FUSE mount:

1. The FUSE session is torn down
2. The daemon automatically exits when the last mount is removed

# taterfs backend storage model

## Overview

A repository backend stores versioning, directory structure, and file data.  A key goal of the backend format is simplicity: the contents of a repository should be fairly simple to deduce and recover even in a situation where the supporting software no longer exists.

A repository is divided into a set of **objects**.  Each object contains either a "chunk" of a file, or structural information such as directory or versioning information.  File chunks are stored verbatim as extracted from the file (i.e., no compression or other transformations).  Structural objects are stored as JSON objects in [canonical format](https://www.rfc-editor.org/rfc/rfc8785).  Every object is referenced by its **object id**, which is the sha-256 hash of its contents (converted to a string using lower case hexadecimal).  The object id's are used within the JSON objects to reference other objects: e.g., version objects reference directory objects, directory objects reference other directory objects or file objects, file objects reference file "chunks".

Every object is expected to be less than approximately 4MB in size.  This eases the burden on tools that interact with the backend, eliminating the need for large single-file transfers and their associated failure headaches.  Of course, this means that large files or directories will need to be assembled from smaller objects.  A File, for example, is actually a JSON object that lists the "chunks" of a file's data, each of which should be less than 4MB.  But for a very large file, that File JSON object might itself grow larger than 4MB, in which case the JSON itself is broken down into smaller parts that are themselves referenced by another JSON object.  A File, therefore, could end up being a hierarchy of JSON objects that eventually point to file chunks.  Directory JSON objects are similar, in that a directory with a huge number of entries may end up being represented by a hierarchy of JSON objects.

Repository versioning follows a **commit** model, in which each commit is a JSON object that references the directory at the root of the file hierarchy, and also references the commit that came before it.  Although each commit conceptually points to a completely new file hierarchy, most of the directory and file objects will be shared between commits since they'll be using many of the same object id's.

The storage model also includes a branching model, in which multiple branches can coexist within the same Repository, each referencing a different commit as its current state.


## Backend Storage JSON Formats

### Root

A repository references the object id of a single Root object.  A new Root is created with every change, such as adding a new commit, or managing the list of branches.

```
interface Root {
  type: "Root"
  timestamp: "*creation time in ISO 8601 format*"
  defaultBranchName: "..."
  defaultBranch: "*object id of default Branch*"
  otherBranches: "*object id of Branches*"
  previousRoot: "*object id of Root*" | null
}
```

### Branches

Branches are simply named commits

```
interface Branches {
  type: "Branches"
  branches: Array<BranchListEntry>
}

enum BranchListEntry {
  Branch(Branch)
  BranchesEntry(BranchesEntry)
}

interface Branch {
  type: "Branch"
  name: "*branch name*"
  commit: "*object id of Commit*"
}

interface BranchesEntry {
  type: "BranchesEntry"
  firstName: string (*name of the first entry in the branches list*)
  lastName: string (*name of the last entry in the branches list*)
  branches: "*object id of Branches*"
}
```

Branches must be listed in lexicographic order.  If there is a large number of branches, then the list must be broken up into a tree of BranchesEntry objects and Branch objects.

### Commits

Commits represent individual versions of the repository.  Each new version is conceptually a completely new directory hierarchy, referenced by a new Commit.  Each Commit also references its immediately-previous commits, thereby providing history.  A Commit with multiple previous Commits typically indicates a merge between two branches - typically the first Commit listed will be the more prominent/durable branch (e.g., the branch into which the merge occurred).

```
interface Commit {
  type: "Commit"
  directory: "*object id of Directory at the root of the version*"
  parents: Array<"*object id of Commit*">
  metadata?: CommitMetadata
}

interface CommitMetadata {
  timestamp?: "*commit time in ISO 8601 format*"
  author?: string | null
  committer?: string | null
  message?: string | null
}
```

### Directories

Conceptually, Directories are lists of entries, each of which points to another Directory or a File.  Entries are ordered alphabetically by UTF.

Because a Directory is effectively unlimited in the number of entries it may contain, a Directory may have to be broken into multiple JSON objects.  To represent this, a directory entry may point to a **DirectoryPart**, which is a separate JSON object that contains some portion of the list of entries.  The DirectoryPart will also contain the names of its first and last entries, which enables faster searching through a Directory.

Even though a Directory may be stored as a full hierarchy of DirectoryParts, conceptually the entire hierarchy is a single list of entries, each of which points to a Directory or a File.

```
interface Directory {
  type: "Directory"
  entries: Array<DirectoryPart>
}

type DirectoryPart = DirectoryEntry | PartialDirectory

type DirectoryEntry = FileEntry | DirEntry

interface FileEntry {
  type: "File"
  name: string
  size: number (*in bytes*)
  executable: boolean
  file: "*object id of File*"
}

interface DirEntry {
  type: "Directory"
  name: string
  directory: "*object id of Directory*"
}

interface PartialDirectory {
  type: "Partial"
  firstName: string (*name of the first entry in the partial list*)
  lastName: string (*name of the last entry in the partial list*)
  directory: "*object id of Directory*"
}
```

### Files

Files are broken down into "chunks" of less than 4MB, each of which contains some portion of the overall file.  The File JSON object lists all of the parts of the original file, allowing that file to be reconstructed from those individual chunks.

For very large files, the File JSON object might itself grow unmanageably large.  In those cases, the list of chunks is itself broken down into *FileFileParts*, each of which contains some portion of the list of chunks.  Conceptually a File is still just an ordered list of chunks, but the list itself might be broken into multiple FileFileParts.

```
interface File {
  type: "File"
  parts: Array<FilePart>
}

type FilePart = FileFilePart | ChunkFilePart

interface ChunkFilePart {
  type: "Chunk"
  size: number (*in bytes*)
  content: "*object id of file chunk*"
}

interface FileFilePart {
  type: "File"
  size: number
  file: "*object id of File*"
}
```

### Conventional Limits

While not strictly required, these are the conventions that are expected of a repository:

The maximum number of entries that will be allowed in a single Directory object before it needs to be broken into multiple PartialDirectories.
```
const MAX_DIRECTORY_ENTRIES = 256
```

The maxmium number of entries that will be allowed in a single File object before it needs to be broken into multiple FileFileParts.
```
const MAX_FILE_PARTS = 64
```

The maxmium number of BranchListEntries that will be allowed in a Branches object before the list must be broken up into mutliple Branches objects.
```
const MAX_BRANCH_LIST_ENTRIES = 64
```

The table for breaking down a file into chunks.  Rather than breaking a file down into 4MB chunks and leaving the remainder as a single chunk, the remainder should be broken down into smaller and smaller parts.  This is an optimization for files that grow from their end, minimizing the changes between one version and the next.

When breaking a file down, the process is to look at how much of the file is remaining, and choose the largest chunk from the table below that is equal to or less than that remaining size.  Continue that process until no entry from the table fits, at which point make a chunk out of whatever portion of the file remains.

```
const CHUNK_SIZES = [
  4194304, // 4Mb
  1048576, // 1Mb
  262144, // 256k
  65536, // 64k
  16384, // 16k
]
```

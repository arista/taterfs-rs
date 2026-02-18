# CLI

The CLI is the main interface to the taterfs-rs package.  It is assumed that the main entry point is named "tfs".

## Common Arguments

As described in [configuration](./configuration.md), these arguments customize how configuration is handled:

```
--config-file={...}
--config-file-overrides={...}
--config {name}={value} (possibly repeated)
```

Other common arguments:

```
--json - format output as JSON
--no-cache - disable caching
```

Some commands require a repo and, in many cases, a file store.  Both of those can be specified explicitly:

```
--repository={repo spec}
--filestore={filestore spec}
```
If not specified explicitly, then there are heuristics to determine these:

* search upward from the current directory for the closest ".tfs/" directory.  If one exists, then the directory containing the .tfs/ directory is the filestore, and the appropriate filestore spec should be generated for it
* if that filestore has a sync_state or a next_sync_state, then the repo specified in that sync state should be used to form the repo spec.

In addition, some commands may require a current path within either the filestore or the repo.  The following heuristics are used:

* if a filestore path is required, and none has been specified:
    * if the filestore is a FsFileStore, and the current directory is within that filestore
        * then use the current directory's name relative to the filestore's root
    * else, use "/"
* if a filestore path is required, and a relative path is specified instead of an absolute one
    * calculate a "base" filestore path using the algorithm above (as if no filestore path was specified)
    * resolve the specified path relative to that "base"
* if a repo path is required, and none has been specified:
    * if the filestore is a FsFileStore, and the current directory is within that filestore, and a StoreSync (either current or next) is associated with the filestore
        * take the current directory relative to the filestore's root, and resolve it relative to the "repository_directory" in the sync state
    * else, use "/"
* if a repo path is required, and a relative path is specified instead of an absolute one
    * calculate a "base" repo path using the algorithm above (as if no repo path was specified)
    * resolve the specified path relative to that "base"

### CommandContext

The CommandContext structure and create_command_context function encapsulate the above behaviors.  The CLI commands should use these functions to process their common command-line arguments (skipping those portions that don't apply)

```
CommandContextInput {
  config_file: Option<string>
  config_file_overrides: Option<string>
  config: Map<name to value>
  json: Option<bool>
  no_cache: Option<bool>
  repository_spec: Option<string>
  file_store_spec: Option<string>
}

CommandContextRequirements {
  require_repository: bool
  require_repository_path: bool
  require_file_store: bool
  require_file_store_path: bool
}

CommandContext {
  config: Config
  json: bool
  no_cache: bool
  repository_spec: Option<string>
  repository_path: Option<string>
  file_store_spec: Option<string>
  file_store_path: Option<string>
}

async create_command_context(input: CommandContextInput, requirements: CommandContextRequirements) -> CommandContext
```


## CLI sub-commands

### tfs repo

```
tfs repo initialize
  [--default-branch-name {default branch name}]
  [--uuid {repository uuid}]
```
Requires a repository spec

Initialize the given repo.  Once initialized, print the repository's RepositoryInfo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.


```
tfs repo get-current-root
  [--output-file / -o {output file}]
```
Requires a repository spec

Print the current root.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.


```
tfs repo get-repository-info
  [--output-file / -o {output file}]
```
Requires a repository spec

Print the repository's RepositoryInfo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.

```
tfs repo set-current-root [{current root}]
  [--input-file / -f {file containing current root}]
```
Requires a repository spec

Set the current root.  If -f is specified, then the current root is read from the given file.  It is an error for both -f and {current root} to be specified.  If neither is specified, then it is read from STDIN.


```
tfs repo exists {object id}
  [--output-file / -o {output file}]
```
Requires a repository spec

Generates "true" or "false" depending if the given object exists in the given repo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.

```
tfs repo read [{object id}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Requires a repository spec

Reads the object at the given id, with an error if it doesn't exist.  If --input-file/-f is specified, then the object id will be read from that file.  It is an error for both -f and {object id} to be specified.  If neither is specified, then the object id will be read from STDIN.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the contents will be written to the given file, otherwise the contents will be written to STDOUT.

```
tfs repo write [{contents}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Requires a repository spec

Writes the given contents to the given repo and generates the resulting hash as output.  If --input-file/-f is specified, then the contents will be read from that file.  It is an error for both -f and {contents} to be specified.  If neither is specified, then the contents will be read from STDIN.  The resulting hash will be written to STDOUT or to the given {output file}, and will be formatted as pretty-printed JSON if --json is specified.

```
tfs repo upload-file {filestore path}
  [--output-file / -o {output file}]
```
Requires a repository spec, filestore spec, and filestore path

Calls upload_file() from the filestore to the repo and prints the resulting file hash id to STDOUT, or the given output file

```
tfs repo upload-directory [{filestore path}]
  [--output-file / -o {output file}]
```
Requires a repository spec, filestore spec, and filestore path.

Calls upload_directory() from the filestore to the repo and prints the resulting directory hash id to STDOUT, or the given output file

```
tfs repo download-directory {directory object_id}
  [--dry-run / -n]
  [--verbose / -v]
  [--no-stage]
  [--output-file / -o {output file}]
```
Requires a repository spec, filestore spec, and filestore path.

Downloads the given directory from the given repo to the given path in the given filestore.  If --dry-run is specified, then the the system will still go through all the steps of seeing what actions would be taken, but no actual changes are made to the filestore.  If verbose is specified, then those actions will be printed as follows:

```
mkdir {path}
rm {path}
download {path} <- {object_id}{if executable then add "+x"}
chmod {"+x" or "-x"} {path} 
```
If "--no-stage" is specified then the download will proceed without using a stage, meaning that files will be downloaded directly into their final destinations, as opposed to downloading all content into a temporary "stage" area before assembling and moving files into their final locations.

```
tfs repo download-file {directory object_id} [{path}]
```
Requires a repository spec, filestore spec, and filestore path.

Downloads the given file object to the given path within the given filestore.

### tfs file-store

```
tfs file-store scan [{path}]
  [--output-file / -o {output file}]
```
Requires a filestore spec, and filestore path.

Calls scan() on the filestore and prints the results to STDOUT, or the given output file.  For each ScanEvent:
  EnterDirectory - print "{name}/" at the current indent level, and increment the current indent level by 4
  ExitDirectory - decrease the current indent level by 4
  File - print "{name} ({size} bytes, {x or - for executable bit}, fingerprint: {fingerprint})" at the current indent level

```
tfs file-store source_chunks [{path}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Requires a filestore spec, and filestore path.

Calls get_source_chunks on the filestore at the given path (which may alternately be supplied through the input file, or through STDIN).  For each resulting source chunk, print:

{offset}+{size}, hash: {hash}

At the end, print:

total: {total size}

### tfs key-value-cache

```
tfs key-value-cache list-entries [{prefix}]
  [--output-file / -o {output file}]
```
Calls list_entries() on the cache, and for each entry, converts the key and value to strings and prints out:

```
{key} -> {value}
```
The output is printed to the output-file is specified, otherwise STDOUT.  If no prefix is specified, then all entries will be printed


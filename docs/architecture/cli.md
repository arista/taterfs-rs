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

## CLI Commands

### tfs repo

```
tfs repo initialize {repo spec}
  [--default-branch-name {default branch name}]
  [--uuid {repository uuid}]
```
Initialize the given repo.  Once initialized, print the repository's RepositoryInfo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.


```
tfs repo get-current-root {repo spec}
  [--output-file / -o {output file}]
```
Print the current root.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.


```
tfs repo get-repository-info {repo spec}
  [--output-file / -o {output file}]
```
Print the repository's RepositoryInfo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.


```
tfs repo set-current-root {repo spec} [{current root}]
  -f {file containing current root}
```
Set the current root.  If -f is specified, then the current root is read from the given file.  It is an error for both, or neither, -f and {current root} to be specified.


```
tfs repo set-current-root {repo spec} [{current root}]
  [--input-file / -f {file containing current root}]
```
Set the current root.  If -f is specified, then the current root is read from the given file.  It is an error for both -f and {current root} to be specified.  If neither is specified, then it is read from STDIN.


```
tfs repo exists {repo spec} {object id}
  [--output-file / -o {output file}]
```
Generates "true" or "false" depending if the given object exists in the given repo.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the generated result will be written to the given file, otherwise the generated results will be written to STDOUT.

```
tfs repo read {repo spec} [{object id}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Reads the object at the given id, with an error if it doesn't exist.  If --input-file/-f is specified, then the object id will be read from that file.  It is an error for both -f and {object id} to be specified.  If neither is specified, then the object id will be read from STDIN.  If "--json" is specified, then the output will be formatted as pretty-printed JSON, otherwise it will remain the raw output.  If {output file} is specified then the contents will be written to the given file, otherwise the contents will be written to STDOUT.

```
tfs repo write {repo spec} [{contents}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Writes the given contents to the given repo and generates the resulting hash as output.  If --input-file/-f is specified, then the contents will be read from that file.  It is an error for both -f and {contents} to be specified.  If neither is specified, then the contents will be read from STDIN.  The resulting hash will be written to STDOUT or to the given {output file}, and will be formatted as pretty-printed JSON if --json is specified.

```
tfs repo upload-file {repo spec} {filestore spec} {path}
  [--output-file / -o {output file}]
```
Calls upload_file() from the filestore to the repo and prints the resulting file hash id to STDOUT, or the given output file

```
tfs repo upload-directory {repo spec} {filestore spec} [{path}]
  [--output-file / -o {output file}]
```
Calls upload_directory() from the filestore to the repo and prints the resulting directory hash id to STDOUT, or the given output file

### tfs file-store

```
tfs file-store scan {filestore spec} [{path}]
  [--output-file / -o {output file}]
```
Calls scan() on the filestore and prints the results to STDOUT, or the given output file.  For each ScanEvent:
  EnterDirectory - print "{name}/" at the current indent level, and increment the current indent level by 4
  ExitDirectory - decrease the current indent level by 4
  File - print "{name} ({size} bytes, {x or - for executable bit}, fingerprint: {fingerprint})" at the current indent level

```
tfs file-store source_chunks {filestore spec} [{path}]
  [--input-file / -f {input file}]
  [--output-file / -o {output file}]
```
Calls get_source_chunks on the filestore at the given path (which may alternately be supplied through the input file, or through STDIN).  For each resulting source chunk, print:

{offset}+{size}, hash: {hash}

At the end, print:

total: {total size}

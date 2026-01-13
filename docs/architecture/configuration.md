# Configuration

The application is configured through the use of an "INI" formatted config file.

## config file location

The following rules are applied to find the config file:

* If the config file location is specified as part of the CLI parameters, then that will be used (and will error if it doesn't exist)
* If environment variable TFS_CONFIG_FILE is specified, then that will be used (if it specifies a nonexistent file, then a warning will be printed)
* If ~/.tfsconfig exists, then that will be used
* Otherwise, no config file is used

## config file contents

* [cache] section - configuration of the durable cache stored locally
    * path={default /tmp/tfsconfig-cache}
    * no-cache={true|false, default false} - if true, then the caches are bypassed

* [memory] section - configuration and maximums that will be applied to operations that potentially allocate and hold memory.  "none" may be specifed, in which case no capacity manager is used for memory limits
    * max={default 100MB}

* [network] section - configuration and maximums that will be used for capacity managers that are applied to all network-based repositories and file stores.  Each can optionally specify "none", in which case no capacity manager is used for that aspect
    * max_concurrent_requests={default 40}
    * max_requests_per_second={default 100}
    * max_read_bytes_per_second={default 100MB}
    * max_write_bytes_per_second={default 100MB}
    * max_total_bytes_per_second={default "none"}
    
* [s3] section - specify defaults and maximums that will be applied to all s3-based repositories and file stores in place of the capacity managers described in [network].  Parameters that are not specified will inherit the corresponding parameter or corresponding capacity manager described in [network].  A capacity manager parameter may specify "none", in which case no capacity manager will be used.
    * endpoint_url={default None}
    * region={default None}
    * max_concurrent_requests={default unspecified}
    * max_requests_per_second={default unspecified}
    * max_read_bytes_per_second={default unspecified}
    * max_write_bytes_per_second={default unspecified}
    * max_total_bytes_per_second={default unspecified}
    
* [repository.{repository name}] - defines a named repository.  Specifies defaults and maximums that will be applied to just this repository using repository-specific capacity managers.  Parameters that are not specified will inherit the corresponding parameter or capacity manager described in [s3] or [network].  A capacity manager parameter may specify "none", in which case no capacity manager will be used.
    * url={repository url, required}
    * endpoint_url={default unspecified}
    * region={default unspecified}
    * max_concurrent_requests={default unspecified}
    * max_requests_per_second={default unspecified}
    * max_read_bytes_per_second={default unspecified}
    * max_write_bytes_per_second={default unspecified}
    * max_total_bytes_per_second={default unspecified}

* [filestore.{filestore name}] - defines a named file store.  Specifies defaults and maximums that will be applied to just this file store using file store-specific capacity managers.  Parameters that are not specified will inherit the corresponding parameter or capacity manager described in [s3] or [network].  A capacity manager parameter may specify "none", in which case no capacity manager will be used.
    * url={file store url, required}
    * endpoint_url={default unspecified}
    * region={default unspecified}
    * max_concurrent_requests={default unspecified}
    * max_requests_per_second={default unspecified}
    * max_read_bytes_per_second={default unspecified}
    * max_write_bytes_per_second={default unspecified}
    * max_total_bytes_per_second={default unspecified}

## command-line config file overrides

TODO: determine how command line overrides should work


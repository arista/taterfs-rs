# Repository Specification

When the application runs, it will likely need to use configured Repo interfaces connected to particular backends, caches, and capacity managers.  The basic way to refer to a repository is through a URL:

```
s3://{bucket}/{prefix}
file://{directory}
http://...
```

Each of those URL's would lead to the creation of a Repo connected to an FsBackend, S3Backend, or HttpBackend.  Each type of backend might require additional parmeters.  The S3Backend, for example, can optionally take an endpoint_url and a region.

The repos might then need 

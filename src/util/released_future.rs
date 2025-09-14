// A ReleasedFuture represents an async operation that may be restricted as to when it may start running.  For example, a network read operation might be gated by a throughput limiter.  Such an operation would have two separate future events - the first is when the throughput limiter permits the operation to begin, and the second is when the operation completes.
//
// An async function returning a ReleasedFuture is how a service exposes such operations to callers, allowing callers to regulate their own use of the service.  From a caller's point of view, if an API returns a ReleasedFuture, then the caller is expected to handle the result as follows:
//
// It must first immediately await? the Future returned by the async function.  This will block until the system is prepared to begin the request.  This is how the system applies "backpressure" to upstream operations.
// The first await will yield a ReleasedFuture.  The caller can then do what it wants with that Future.  It can, for example, await it immediately, effectively blocking until the operation is complete.  Or it can store that ReleasedFuture away to await later, or combine with multiple ReleasedFutures using, for example, try_join_all.
//
// Note that all of this implies a single-threaded context, so Rc<> can be used instead of Arc<>, for example, and no values are required to be Send or Sync.  It also requires 'static context, meaning that everything the async function uses internally must be 'static - it can't depend on some local frame not disappearing.  Things like Rc<> are ok, borrows may be less so.

use futures_util::future::LocalBoxFuture;

pub type ReleasedFuture<T> = anyhow::Result<LocalBoxFuture<'static, anyhow::Result<T>>>;

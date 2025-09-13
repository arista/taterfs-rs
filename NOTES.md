Some implementation notes:

# single-threaded context

This makes heavy use of async functions to manage many parallel operations.  However, all of the application code runs in a single-threaded context.  The application code does launch I/O operations that themselves might be implemented by the system to use multiple threads, but all application-level interactions occur within a single thread.  This has a few implications:

* The code can freely use Rc<> instead of Arc<>, which is a significant performance improvement
* Nothing is required to implement Send or Sync
* The main function does use tokio to implement the async system, but it confines tokio to a single-threaded context ('#[tokio::main(flavor = "current_thread")']
* Traits that use async can be declared as "#[async_trait(?Send)]", meaning that their futures don't need to be Send
* When packaging a future for later use, the code can use "LocalBoxFuture" instead of "BoxFuture", which doesn't require the future to be Send.
* If an async block within a function needs to become a LocalBoxFuture, it should use ".boxed_local()"

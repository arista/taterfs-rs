# Completes

There are several operations that initiate multiple concurrent processes, then return preliminary results of those processes without waiting for all of those processes to fully complete.  Those operations will instead return a struct containing both the preliminary results, plus an async "complete flag" that the caller can use to wait for all the processes to complete.  Or the caller can "roll up" that complete flag with other flags to form a new complete flag that it can then return to its own caller.

## Interfaces

```
trait Complete {
  async complete()
}

// An implementation of Complete, whose complete() function will return when notify_complete() is called
NotifyComplete {
  notify_complete()
}

// An implementation of Complete which accepts multiple Completes to be added.  Its complete() function will return when all of its added Completes are complete, **and** done() has been called.  It is an error to call add() after done() has been called.
Completes {
  add(complete: Arc<Complete>)
  done()
}

```

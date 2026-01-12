# Flow Control

Several components require some level of limiting or flow control:

* Repository backends that use the network, such as the S3Backend, may want to limit their network throughput, concurrent request counts, and request rates.
* Multiple concurrent processes reading and writing data may want to limit their actions based on the amount of memory available to buffer that data.

## CapacityManager

All of these limitations can be abstracted into a CapacityManager with this interface:

```
async use(amount: u64) -> UsedCapacity
```

The CapacityManager is configured with a u64 limit, and maintains a "used" u64 which starts at 0.  As use() calls are made, the "used" value increases appropriately.  As the UsedCapacity values are Drop'ed the "used" value decreases appropriately (but never goes below 0).

If the "used" value is at or above the "limit", then the next use() requests are placed onto a queue.  If the "used" value later goes below the limit (because UsedCapacity's are Drop'ed), then the next use() requests in the queue will be fulfilled until the "used" value reaches the "limit" again.

The CapacityManager can also be configured with a replenishment rate:

```
interface ReplenishmentRate {
  amount: u64
  period: *time period*
}
```

If this is supplied, then the CapacityManager will automatically call replenish(amount) every period.

## Applications

The CapacityManager will likely be configured and used in these ways:

### Network Bandwidth Limiter

This will be a CapacityManager configured with a replenishment rate that corresponds to the desired bandwidth.  It will also be configured with a "limit" that represents a burstable amount.  The application will generally only call use(), and will rely on the automatic replenishment.

There will likely be separate send and receive bandwidth limiters, managed globally by the application.

### Memory Usage Limiter

This will be a CapacityManager with no replenisment rate, whose limit defines the amount of memory that should be used for particular applications (such as reading and writing data).

### Request Rate Limiter

This will be a CapacityManager configured with a replenishment rate that corresponds to the desired request rate.  It will also be configured with a "limit" that represents a burstable amount.  The application will generally only call use(), and will rely on the automatic replenishment.

### Concurrent Request Limiter

This will be a CapacityManager used to limit the number of concurrent processes or requests.  It will be configured with no replenishment rate, and with a limit that represents the number of concurrent processes.


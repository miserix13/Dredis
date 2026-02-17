using System.Collections.Generic;

namespace Dredis.Abstractions.Storage
{
    /// <summary>
    /// Defines conditional semantics for setting string values.
    /// </summary>
    public enum SetCondition
    {
        None,
        Nx,
        Xx
    }

    /// <summary>
    /// Represents a stream entry with fields and values.
    /// </summary>
    public sealed class StreamEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamEntry"/> class.
        /// </summary>
        /// <param name="id">The stream entry id.</param>
        /// <param name="fields">The field/value pairs for the entry.</param>
        public StreamEntry(string id, KeyValuePair<string, byte[]>[] fields)
        {
            Id = id;
            Fields = fields;
        }

        /// <summary>
        /// Gets the stream entry id.
        /// </summary>
        public string Id { get; }
        /// <summary>
        /// Gets the field/value pairs for the entry.
        /// </summary>
        public KeyValuePair<string, byte[]>[] Fields { get; }
    }

    /// <summary>
    /// Represents stream read results for a single stream.
    /// </summary>
    public sealed class StreamReadResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReadResult"/> class.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="entries">The stream entries.</param>
        public StreamReadResult(string key, StreamEntry[] entries)
        {
            Key = key;
            Entries = entries;
        }

        /// <summary>
        /// Gets the stream key.
        /// </summary>
        public string Key { get; }
        /// <summary>
        /// Gets the entries returned for the stream.
        /// </summary>
        public StreamEntry[] Entries { get; }
    }

    /// <summary>
    /// Describes results of creating a consumer group.
    /// </summary>
    public enum StreamGroupCreateResult
    {
        Ok,
        Exists,
        NoStream,
        WrongType,
        InvalidId
    }

    /// <summary>
    /// Describes results of destroying a consumer group.
    /// </summary>
    public enum StreamGroupDestroyResult
    {
        Removed,
        NotFound,
        WrongType
    }

    /// <summary>
    /// Describes results of list operations.
    /// </summary>
    public enum ListResultStatus
    {
        Ok,
        WrongType
    }

    /// <summary>
    /// Represents a list push result.
    /// </summary>
    public sealed class ListPushResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListPushResult"/> class.
        /// </summary>
        public ListPushResult(ListResultStatus status, long length)
        {
            Status = status;
            Length = length;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListResultStatus Status { get; }
        /// <summary>
        /// Gets the list length after the push.
        /// </summary>
        public long Length { get; }
    }

    /// <summary>
    /// Represents a list pop result.
    /// </summary>
    public sealed class ListPopResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListPopResult"/> class.
        /// </summary>
        public ListPopResult(ListResultStatus status, byte[]? value)
        {
            Status = status;
            Value = value;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListResultStatus Status { get; }
        /// <summary>
        /// Gets the popped value, or null if empty.
        /// </summary>
        public byte[]? Value { get; }
    }

    /// <summary>
    /// Represents a list range result.
    /// </summary>
    public sealed class ListRangeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListRangeResult"/> class.
        /// </summary>
        public ListRangeResult(ListResultStatus status, byte[][] values)
        {
            Status = status;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListResultStatus Status { get; }
        /// <summary>
        /// Gets the values in the requested range.
        /// </summary>
        public byte[][] Values { get; }
    }

    /// <summary>
    /// Represents a list length result.
    /// </summary>
    public sealed class ListLengthResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListLengthResult"/> class.
        /// </summary>
        public ListLengthResult(ListResultStatus status, long length)
        {
            Status = status;
            Length = length;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListResultStatus Status { get; }
        /// <summary>
        /// Gets the list length.
        /// </summary>
        public long Length { get; }
    }

    /// <summary>
    /// Represents a list index lookup result.
    /// </summary>
    public sealed class ListIndexResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListIndexResult"/> class.
        /// </summary>
        public ListIndexResult(ListResultStatus status, byte[]? value)
        {
            Status = status;
            Value = value;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListResultStatus Status { get; }
        /// <summary>
        /// Gets the indexed value, or null if out of range.
        /// </summary>
        public byte[]? Value { get; }
    }

    /// <summary>
    /// Describes results of setting a list index.
    /// </summary>
    public enum ListSetResultStatus
    {
        Ok,
        WrongType,
        OutOfRange
    }

    /// <summary>
    /// Represents a list set result.
    /// </summary>
    public sealed class ListSetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ListSetResult"/> class.
        /// </summary>
        public ListSetResult(ListSetResultStatus status)
        {
            Status = status;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ListSetResultStatus Status { get; }
    }

    /// <summary>
    /// Describes results of set operations.
    /// </summary>
    public enum SetResultStatus
    {
        Ok,
        WrongType
    }

    /// <summary>
    /// Represents a set operation result with a count.
    /// </summary>
    public sealed class SetCountResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SetCountResult"/> class.
        /// </summary>
        public SetCountResult(SetResultStatus status, long count)
        {
            Status = status;
            Count = count;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SetResultStatus Status { get; }
        /// <summary>
        /// Gets the count for the operation.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Represents a set members result.
    /// </summary>
    public sealed class SetMembersResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SetMembersResult"/> class.
        /// </summary>
        public SetMembersResult(SetResultStatus status, byte[][] members)
        {
            Status = status;
            Members = members;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SetResultStatus Status { get; }
        /// <summary>
        /// Gets the members in the set.
        /// </summary>
        public byte[][] Members { get; }
    }

    /// <summary>
    /// Describes results of setting a stream's last id.
    /// </summary>
    public enum StreamSetIdResultStatus
    {
        Ok,
        WrongType,
        InvalidId
    }

    /// <summary>
    /// Describes results of setting a consumer group last id.
    /// </summary>
    public enum StreamGroupSetIdResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType,
        InvalidId
    }

    /// <summary>
    /// Describes results of removing a consumer from a group.
    /// </summary>
    public enum StreamGroupDelConsumerResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType
    }

    /// <summary>
    /// Represents a consumer group delconsumer result.
    /// </summary>
    public sealed class StreamGroupDelConsumerResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamGroupDelConsumerResult"/> class.
        /// </summary>
        public StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus status, long removed)
        {
            Status = status;
            Removed = removed;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public StreamGroupDelConsumerResultStatus Status { get; }
        /// <summary>
        /// Gets the number of pending entries removed.
        /// </summary>
        public long Removed { get; }
    }

    /// <summary>
    /// Describes results of reading from a consumer group.
    /// </summary>
    public enum StreamGroupReadResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType,
        InvalidId
    }

    /// <summary>
    /// Describes results of acknowledging consumer group entries.
    /// </summary>
    public enum StreamAckResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType
    }

    /// <summary>
    /// Represents a consumer group read operation result.
    /// </summary>
    public sealed class StreamGroupReadResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamGroupReadResult"/> class.
        /// </summary>
        /// <param name="status">The read status.</param>
        /// <param name="results">The stream read results.</param>
        public StreamGroupReadResult(StreamGroupReadResultStatus status, StreamReadResult[] results)
        {
            Status = status;
            Results = results;
        }

        /// <summary>
        /// Gets the read status.
        /// </summary>
        public StreamGroupReadResultStatus Status { get; }
        /// <summary>
        /// Gets the stream read results.
        /// </summary>
        public StreamReadResult[] Results { get; }
    }

    /// <summary>
    /// Represents a consumer group acknowledgment result.
    /// </summary>
    public sealed class StreamAckResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamAckResult"/> class.
        /// </summary>
        /// <param name="status">The acknowledgment status.</param>
        /// <param name="count">The number of entries acknowledged.</param>
        public StreamAckResult(StreamAckResultStatus status, long count)
        {
            Status = status;
            Count = count;
        }

        /// <summary>
        /// Gets the acknowledgment status.
        /// </summary>
        public StreamAckResultStatus Status { get; }
        /// <summary>
        /// Gets the number of entries acknowledged.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Describes results of a pending entries query.
    /// </summary>
    public enum StreamPendingResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType
    }

    /// <summary>
    /// Represents a pending entry in a consumer group.
    /// </summary>
    public sealed class StreamPendingEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamPendingEntry"/> class.
        /// </summary>
        /// <param name="id">The entry id.</param>
        /// <param name="consumer">The consumer name.</param>
        /// <param name="idleTimeMs">The idle time in milliseconds.</param>
        /// <param name="deliveryCount">The delivery count.</param>
        public StreamPendingEntry(string id, string consumer, long idleTimeMs, long deliveryCount)
        {
            Id = id;
            Consumer = consumer;
            IdleTimeMs = idleTimeMs;
            DeliveryCount = deliveryCount;
        }

        /// <summary>
        /// Gets the entry id.
        /// </summary>
        public string Id { get; }
        /// <summary>
        /// Gets the consumer name.
        /// </summary>
        public string Consumer { get; }
        /// <summary>
        /// Gets the idle time in milliseconds.
        /// </summary>
        public long IdleTimeMs { get; }
        /// <summary>
        /// Gets the delivery count.
        /// </summary>
        public long DeliveryCount { get; }
    }

    /// <summary>
    /// Summarizes pending entries for a consumer.
    /// </summary>
    public sealed class StreamPendingConsumerInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamPendingConsumerInfo"/> class.
        /// </summary>
        /// <param name="name">The consumer name.</param>
        /// <param name="count">The pending entry count.</param>
        public StreamPendingConsumerInfo(string name, long count)
        {
            Name = name;
            Count = count;
        }

        /// <summary>
        /// Gets the consumer name.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Gets the pending entry count.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Represents the result of a pending entries query.
    /// </summary>
    public sealed class StreamPendingResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamPendingResult"/> class.
        /// </summary>
        /// <param name="status">The result status.</param>
        /// <param name="count">The total pending count.</param>
        /// <param name="smallestId">The smallest pending id.</param>
        /// <param name="largestId">The largest pending id.</param>
        /// <param name="consumers">The per-consumer counts.</param>
        /// <param name="entries">The pending entries (extended form).</param>
        public StreamPendingResult(
            StreamPendingResultStatus status,
            long count,
            string? smallestId,
            string? largestId,
            StreamPendingConsumerInfo[] consumers,
            StreamPendingEntry[] entries)
        {
            Status = status;
            Count = count;
            SmallestId = smallestId;
            LargestId = largestId;
            Consumers = consumers;
            Entries = entries;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public StreamPendingResultStatus Status { get; }
        /// <summary>
        /// Gets the total pending count.
        /// </summary>
        public long Count { get; }
        /// <summary>
        /// Gets the smallest pending id, if available.
        /// </summary>
        public string? SmallestId { get; }
        /// <summary>
        /// Gets the largest pending id, if available.
        /// </summary>
        public string? LargestId { get; }
        /// <summary>
        /// Gets per-consumer pending counts.
        /// </summary>
        public StreamPendingConsumerInfo[] Consumers { get; }
        /// <summary>
        /// Gets pending entries when using extended form.
        /// </summary>
        public StreamPendingEntry[] Entries { get; }
    }

    /// <summary>
    /// Describes results of claiming pending entries.
    /// </summary>
    public enum StreamClaimResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType
    }

    /// <summary>
    /// Represents the result of a claim operation.
    /// </summary>
    public sealed class StreamClaimResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamClaimResult"/> class.
        /// </summary>
        /// <param name="status">The claim status.</param>
        /// <param name="entries">The claimed entries.</param>
        public StreamClaimResult(StreamClaimResultStatus status, StreamEntry[] entries)
        {
            Status = status;
            Entries = entries;
        }

        /// <summary>
        /// Gets the claim status.
        /// </summary>
        public StreamClaimResultStatus Status { get; }
        /// <summary>
        /// Gets the claimed entries.
        /// </summary>
        public StreamEntry[] Entries { get; }
    }

    /// <summary>
    /// Describes results for stream info operations.
    /// </summary>
    public enum StreamInfoResultStatus
    {
        Ok,
        NoGroup,
        NoStream,
        WrongType
    }

    /// <summary>
    /// Represents stream metadata for XINFO STREAM.
    /// </summary>
    public sealed class StreamInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamInfo"/> class.
        /// </summary>
        public StreamInfo(long length, string? lastGeneratedId, StreamEntry? firstEntry, StreamEntry? lastEntry)
        {
            Length = length;
            LastGeneratedId = lastGeneratedId;
            FirstEntry = firstEntry;
            LastEntry = lastEntry;
        }

        /// <summary>
        /// Gets the stream length.
        /// </summary>
        public long Length { get; }
        /// <summary>
        /// Gets the last generated id.
        /// </summary>
        public string? LastGeneratedId { get; }
        /// <summary>
        /// Gets the first entry, if available.
        /// </summary>
        public StreamEntry? FirstEntry { get; }
        /// <summary>
        /// Gets the last entry, if available.
        /// </summary>
        public StreamEntry? LastEntry { get; }
    }

    /// <summary>
    /// Represents stream group metadata for XINFO GROUPS.
    /// </summary>
    public sealed class StreamGroupInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamGroupInfo"/> class.
        /// </summary>
        public StreamGroupInfo(string name, long consumers, long pending, string lastDeliveredId)
        {
            Name = name;
            Consumers = consumers;
            Pending = pending;
            LastDeliveredId = lastDeliveredId;
        }

        /// <summary>
        /// Gets the group name.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Gets the number of consumers.
        /// </summary>
        public long Consumers { get; }
        /// <summary>
        /// Gets the number of pending entries.
        /// </summary>
        public long Pending { get; }
        /// <summary>
        /// Gets the last delivered id.
        /// </summary>
        public string LastDeliveredId { get; }
    }

    /// <summary>
    /// Represents consumer metadata for XINFO CONSUMERS.
    /// </summary>
    public sealed class StreamConsumerInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamConsumerInfo"/> class.
        /// </summary>
        public StreamConsumerInfo(string name, long pending, long idleTimeMs)
        {
            Name = name;
            Pending = pending;
            IdleTimeMs = idleTimeMs;
        }

        /// <summary>
        /// Gets the consumer name.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// Gets the pending entry count.
        /// </summary>
        public long Pending { get; }
        /// <summary>
        /// Gets the idle time in milliseconds.
        /// </summary>
        public long IdleTimeMs { get; }
    }

    /// <summary>
    /// Represents a result for XINFO STREAM.
    /// </summary>
    public sealed class StreamInfoResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamInfoResult"/> class.
        /// </summary>
        public StreamInfoResult(StreamInfoResultStatus status, StreamInfo? info)
        {
            Status = status;
            Info = info;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public StreamInfoResultStatus Status { get; }
        /// <summary>
        /// Gets the stream info payload.
        /// </summary>
        public StreamInfo? Info { get; }
    }

    /// <summary>
    /// Represents a result for XINFO GROUPS.
    /// </summary>
    public sealed class StreamGroupsInfoResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamGroupsInfoResult"/> class.
        /// </summary>
        public StreamGroupsInfoResult(StreamInfoResultStatus status, StreamGroupInfo[] groups)
        {
            Status = status;
            Groups = groups;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public StreamInfoResultStatus Status { get; }
        /// <summary>
        /// Gets the group info list.
        /// </summary>
        public StreamGroupInfo[] Groups { get; }
    }

    /// <summary>
    /// Represents a result for XINFO CONSUMERS.
    /// </summary>
    public sealed class StreamConsumersInfoResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamConsumersInfoResult"/> class.
        /// </summary>
        public StreamConsumersInfoResult(StreamInfoResultStatus status, StreamConsumerInfo[] consumers)
        {
            Status = status;
            Consumers = consumers;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public StreamInfoResultStatus Status { get; }
        /// <summary>
        /// Gets the consumer info list.
        /// </summary>
        public StreamConsumerInfo[] Consumers { get; }
    }

    /// <summary>
    /// Key-Value Storage abstraction for Dredis.
    /// </summary>
    public interface IKeyValueStore
    {
        /// <summary>
        /// Asynchronously retrieves the value associated with the specified key from the key-value store.
        /// </summary>
        /// <remarks>If the operation is canceled, the returned task will complete with a
        /// TaskCanceledException. Callers should handle this exception as appropriate for their scenario.</remarks>
        /// <param name="key">The key used to identify the value to retrieve. This parameter cannot be null or empty.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation. The default value is CancellationToken.None.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the value associated with the
        /// specified key as a byte array, or null if the key does not exist.</returns>
        Task<byte[]?> GetAsync(string key, CancellationToken token = default);
        /// <summary>
        /// Asynchronously sets the value associated with the specified key in the cache, optionally specifying an
        /// expiration time.
        /// </summary>
        /// <remarks>An exception may be thrown if the key is invalid or if the operation is canceled.
        /// Ensure that both the key and value are valid before calling this method.</remarks>
        /// <param name="key">The unique key to associate with the cached value. This parameter cannot be null or empty.</param>
        /// <param name="value">The value to store in the cache as a byte array. This parameter cannot be null.</param>
        /// <param name="expiration">An optional time interval after which the cached value expires and is removed. If null, the value does not
        /// expire.</param>
        /// <param name="condition">A conditional flag that controls whether the key is set.</param>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <see langword="true"/> if the value
        /// was successfully set; otherwise, <see langword="false"/>.</returns>
        Task<bool> SetAsync(
            string key,
            byte[] value,
            TimeSpan? expiration,
            SetCondition condition,
            CancellationToken token = default);

        /// <summary>
        /// Asynchronously retrieves values for multiple keys. The result preserves input order and uses null for missing keys.
        /// </summary>
        /// <param name="keys">The keys to retrieve.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains values aligned with input keys.</returns>
        Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default);

        /// <summary>
        /// Asynchronously sets multiple key-value pairs.
        /// </summary>
        /// <param name="items">Key-value pairs to set.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if all values were set.</returns>
        Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default);
        /// <summary>
        /// Asynchronously deletes the items identified by the specified keys.
        /// </summary>
        /// <remarks>If the operation is canceled, the returned count may not reflect the actual number of
        /// items deleted. This method does not throw an exception if a key does not exist; such keys are
        /// ignored.</remarks>
        /// <param name="keys">An array of strings that represent the unique keys of the items to delete. Each key must correspond to an
        /// existing item. Cannot be null.</param>
        /// <param name="token">A cancellation token that can be used to cancel the delete operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of items that were
        /// successfully deleted.</returns>
        Task<long> DeleteAsync(string[] keys, CancellationToken token = default);
        /// <summary>
        /// Determines whether or not a specific key exists.
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the key exists.</returns>
        Task<bool> ExistsAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Determines how many of the specified keys exist.
        /// </summary>
        /// <param name="keys">Keys to check.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the count of existing keys.</returns>
        Task<long> ExistsAsync(string[] keys, CancellationToken token = default);

        /// <summary>
        /// Increments the integer value stored at key by the given delta. Missing keys are created.
        /// Returns null if the value is not a valid integer or on overflow.
        /// </summary>
        /// <param name="key">The key to increment.</param>
        /// <param name="delta">The delta to add (can be negative).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is the new value or null on error.</returns>
        Task<long?> IncrByAsync(string key, long delta, CancellationToken token = default);
        /// <summary>
        /// Asynchronously sets an expiration time for the cache entry associated with the specified key.
        /// </summary>
        /// <remarks>Use this method to manage cache memory by expiring entries after a specified
        /// duration. If the key does not correspond to an existing cache entry, the method returns <see
        /// langword="false"/>.</remarks>
        /// <param name="key">The unique identifier of the cache entry to expire. This value cannot be null or empty.</param>
        /// <param name="expiration">The duration after which the cache entry should expire. Must be a positive time interval.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <see langword="true"/> if the cache
        /// entry was successfully set to expire; otherwise, <see langword="false"/>.</returns>
        Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default);

        /// <summary>
        /// Asynchronously sets an expiration time (in milliseconds) for the cache entry associated with the specified key.
        /// </summary>
        /// <param name="key">The unique identifier of the cache entry to expire. This value cannot be null or empty.</param>
        /// <param name="expiration">The duration after which the cache entry should expire. Must be a positive time interval.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the cache entry was set to expire.</returns>
        Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default);
        /// <summary>
        /// Asynchronously retrieves the time-to-live (TTL) value, in seconds, for the specified key.
        /// </summary>
        /// <remarks>Use this method to determine how long a key will remain in the store before it is
        /// automatically deleted. If the key does not exist, the method returns -2. If the key has no expiration,
        /// the method returns -1.</remarks>
        /// <param name="key">The key whose TTL value is to be retrieved. The key must exist in the store.</param>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the TTL value in seconds.</returns>
        Task<long> TtlAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Asynchronously retrieves the time-to-live (PTTL) value, in milliseconds, for the specified key.
        /// </summary>
        /// <remarks>If the key does not exist, the method returns -2. If the key has no expiration,
        /// the method returns -1.</remarks>
        /// <param name="key">The key whose PTTL value is to be retrieved. The key must exist in the store.</param>
        /// <param name="token">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the PTTL value in milliseconds.</returns>
        Task<long> PttlAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Sets a hash field to the specified value. Returns true if the field was newly created.
        /// </summary>
        /// <param name="key">The hash key.</param>
        /// <param name="field">The hash field name.</param>
        /// <param name="value">The hash field value.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the field was added.</returns>
        Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default);

        /// <summary>
        /// Retrieves the value for a hash field, or null if missing.
        /// </summary>
        /// <param name="key">The hash key.</param>
        /// <param name="field">The hash field name.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the field value, or null.</returns>
        Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default);

        /// <summary>
        /// Removes one or more fields from a hash.
        /// </summary>
        /// <param name="key">The hash key.</param>
        /// <param name="fields">The hash field names.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of fields removed.</returns>
        Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default);

        /// <summary>
        /// Retrieves all fields and values from a hash.
        /// </summary>
        /// <param name="key">The hash key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains field/value pairs.</returns>
        Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Pushes one or more values onto a list.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="values">The values to push.</param>
        /// <param name="left">True to push to the head, false to push to the tail.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new length.</returns>
        Task<ListPushResult> ListPushAsync(
            string key,
            byte[][] values,
            bool left,
            CancellationToken token = default);

        /// <summary>
        /// Pops a value from a list.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="left">True to pop from the head, false to pop from the tail.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the popped value.</returns>
        Task<ListPopResult> ListPopAsync(
            string key,
            bool left,
            CancellationToken token = default);

        /// <summary>
        /// Returns a range of values from a list.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="start">The starting index.</param>
        /// <param name="stop">The ending index (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains range values.</returns>
        Task<ListRangeResult> ListRangeAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default);

        /// <summary>
        /// Returns the length of a list.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains list length.</returns>
        Task<ListLengthResult> ListLengthAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Returns the value at a list index.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="index">The index to read.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the value.</returns>
        Task<ListIndexResult> ListIndexAsync(
            string key,
            int index,
            CancellationToken token = default);

        /// <summary>
        /// Sets the value at a list index.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="index">The index to update.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status.</returns>
        Task<ListSetResult> ListSetAsync(
            string key,
            int index,
            byte[] value,
            CancellationToken token = default);

        /// <summary>
        /// Trims a list to the specified range.
        /// </summary>
        /// <param name="key">The list key.</param>
        /// <param name="start">The starting index.</param>
        /// <param name="stop">The ending index (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status.</returns>
        Task<ListResultStatus> ListTrimAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default);

        /// <summary>
        /// Adds one or more members to a set.
        /// </summary>
        /// <param name="key">The set key.</param>
        /// <param name="members">Members to add.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number added.</returns>
        Task<SetCountResult> SetAddAsync(
            string key,
            byte[][] members,
            CancellationToken token = default);

        /// <summary>
        /// Removes one or more members from a set.
        /// </summary>
        /// <param name="key">The set key.</param>
        /// <param name="members">Members to remove.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number removed.</returns>
        Task<SetCountResult> SetRemoveAsync(
            string key,
            byte[][] members,
            CancellationToken token = default);

        /// <summary>
        /// Returns all members of a set.
        /// </summary>
        /// <param name="key">The set key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains members.</returns>
        Task<SetMembersResult> SetMembersAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Returns the cardinality of a set.
        /// </summary>
        /// <param name="key">The set key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the set count.</returns>
        Task<SetCountResult> SetCardinalityAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Adds a stream entry. Returns the entry id, or null if the id is invalid or out of order.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="id">The entry id or '*'.</param>
        /// <param name="fields">Field/value pairs for the entry.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is the entry id, or null on error.</returns>
        Task<string?> StreamAddAsync(
            string key,
            string id,
            KeyValuePair<string, byte[]>[] fields,
            CancellationToken token = default);

        /// <summary>
        /// Removes entries by id from a stream.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="ids">Entry ids to remove.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of entries removed.</returns>
        Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default);

        /// <summary>
        /// Returns the length of the stream.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of entries.</returns>
        Task<long> StreamLengthAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Retrieves the last stream entry id for the specified key, or null if the stream is empty.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is the last entry id, or null.</returns>
        Task<string?> StreamLastIdAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Reads entries with ids greater than the provided ids. Result order matches keys.
        /// </summary>
        /// <param name="keys">Stream keys to read.</param>
        /// <param name="ids">Ids to read after (one per key).</param>
        /// <param name="count">Optional per-stream count limit.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains stream entries.</returns>
        Task<StreamReadResult[]> StreamReadAsync(
            string[] keys,
            string[] ids,
            int? count,
            CancellationToken token = default);

        /// <summary>
        /// Returns entries in a stream within the specified id range.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="start">Start id or '-'.</param>
        /// <param name="end">End id or '+'.</param>
        /// <param name="count">Optional maximum number of entries.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains stream entries.</returns>
        Task<StreamEntry[]> StreamRangeAsync(
            string key,
            string start,
            string end,
            int? count,
            CancellationToken token = default);

        /// <summary>
        /// Returns entries in a stream within the specified id range, in reverse order.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="start">Start id or '+'.</param>
        /// <param name="end">End id or '-'.</param>
        /// <param name="count">Optional maximum number of entries.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains stream entries.</returns>
        Task<StreamEntry[]> StreamRangeReverseAsync(
            string key,
            string start,
            string end,
            int? count,
            CancellationToken token = default);

        /// <summary>
        /// Trims a stream by max length or minimum id and returns the number of entries removed.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="maxLength">Optional maximum length for MAXLEN trimming.</param>
        /// <param name="minId">Optional minimum id for MINID trimming.</param>
        /// <param name="approximate">If true, use approximate trimming semantics.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of entries removed.</returns>
        Task<long> StreamTrimAsync(
            string key,
            int? maxLength = null,
            string? minId = null,
            bool approximate = false,
            CancellationToken token = default);

        /// <summary>
        /// Returns stream metadata for XINFO STREAM.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains stream info.</returns>
        Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Returns group metadata for XINFO GROUPS.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains group info.</returns>
        Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default);

        /// <summary>
        /// Returns consumer metadata for XINFO CONSUMERS.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains consumer info.</returns>
        Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(
            string key,
            string group,
            CancellationToken token = default);

        /// <summary>
        /// Sets the stream last generated id.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="lastId">The last generated id.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the status.</returns>
        Task<StreamSetIdResultStatus> StreamSetIdAsync(
            string key,
            string lastId,
            CancellationToken token = default);

        /// <summary>
        /// Creates a consumer group for a stream.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="startId">The start id, '-' or '$'.</param>
        /// <param name="mkStream">Creates the stream if it does not exist.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result describes the create outcome.</returns>
        Task<StreamGroupCreateResult> StreamGroupCreateAsync(
            string key,
            string group,
            string startId,
            bool mkStream,
            CancellationToken token = default);

        /// <summary>
        /// Destroys a consumer group for a stream.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result describes the destroy outcome.</returns>
        Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(
            string key,
            string group,
            CancellationToken token = default);

        /// <summary>
        /// Sets the last delivered id for a consumer group.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="lastId">The last delivered id.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the status.</returns>
        Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(
            string key,
            string group,
            string lastId,
            CancellationToken token = default);

        /// <summary>
        /// Removes a consumer from a group.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="consumer">The consumer name.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains removal details.</returns>
        Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(
            string key,
            string group,
            string consumer,
            CancellationToken token = default);

        /// <summary>
        /// Reads entries for a consumer group.
        /// </summary>
        /// <param name="group">The consumer group name.</param>
        /// <param name="consumer">The consumer name.</param>
        /// <param name="keys">Stream keys to read.</param>
        /// <param name="ids">Ids to read (one per key). Use ">" for new messages.</param>
        /// <param name="count">Optional per-stream count limit.</param>
        /// <param name="block">Optional blocking timeout.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status and entries.</returns>
        Task<StreamGroupReadResult> StreamGroupReadAsync(
            string group,
            string consumer,
            string[] keys,
            string[] ids,
            int? count,
            TimeSpan? block,
            CancellationToken token = default);

        /// <summary>
        /// Acknowledges entries in a consumer group.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="ids">Entry ids to acknowledge.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status and count.</returns>
        Task<StreamAckResult> StreamAckAsync(
            string key,
            string group,
            string[] ids,
            CancellationToken token = default);

        /// <summary>
        /// Returns pending entries in a consumer group.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="minIdleTimeMs">Optional minimum idle time filter in milliseconds.</param>
        /// <param name="start">Optional start id for range query.</param>
        /// <param name="end">Optional end id for range query.</param>
        /// <param name="count">Optional maximum number of entries to return.</param>
        /// <param name="consumer">Optional consumer name filter.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains pending information.</returns>
        Task<StreamPendingResult> StreamPendingAsync(
            string key,
            string group,
            long? minIdleTimeMs = null,
            string? start = null,
            string? end = null,
            int? count = null,
            string? consumer = null,
            CancellationToken token = default);

        /// <summary>
        /// Claims pending entries for a consumer, transferring ownership from other consumers.
        /// </summary>
        /// <param name="key">The stream key.</param>
        /// <param name="group">The consumer group name.</param>
        /// <param name="consumer">The consumer claiming the entries.</param>
        /// <param name="minIdleTimeMs">Minimum idle time in milliseconds for entries to be claimed.</param>
        /// <param name="ids">Entry ids to claim.</param>
        /// <param name="idleMs">Optional idle time to set for claimed entries.</param>
        /// <param name="timeMs">Optional Unix time in milliseconds to set as delivery time.</param>
        /// <param name="retryCount">Optional retry count to set.</param>
        /// <param name="force">If true, creates pending entries even if they don't exist.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status and claimed entries.</returns>
        Task<StreamClaimResult> StreamClaimAsync(
            string key,
            string group,
            string consumer,
            long minIdleTimeMs,
            string[] ids,
            long? idleMs = null,
            long? timeMs = null,
            long? retryCount = null,
            bool force = false,
            CancellationToken token = default);

        /// <summary>
        /// Asynchronously removes all expired keys from the cache.
        /// </summary>
        /// <remarks>The duration of the cleanup operation may vary depending on the number of keys in the
        /// cache. It is recommended to use the cancellation token to avoid long-running operations if
        /// necessary.</remarks>
        /// <param name="token">A cancellation token that can be used to cancel the cleanup operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of keys removed from
        /// the cache.</returns>
        Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default);
    }
}

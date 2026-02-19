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
    /// Describes results of sorted set operations.
    /// </summary>
    public enum SortedSetResultStatus
    {
        Ok,
        WrongType
    }

    /// <summary>
    /// Represents a sorted set entry.
    /// </summary>
    public sealed class SortedSetEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetEntry"/> class.
        /// </summary>
        public SortedSetEntry(byte[] member, double score)
        {
            Member = member;
            Score = score;
        }

        /// <summary>
        /// Gets the member.
        /// </summary>
        public byte[] Member { get; }
        /// <summary>
        /// Gets the score.
        /// </summary>
        public double Score { get; }
    }

    /// <summary>
    /// Represents a sorted set count result.
    /// </summary>
    public sealed class SortedSetCountResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetCountResult"/> class.
        /// </summary>
        public SortedSetCountResult(SortedSetResultStatus status, long count)
        {
            Status = status;
            Count = count;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SortedSetResultStatus Status { get; }
        /// <summary>
        /// Gets the count.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Represents a sorted set range result.
    /// </summary>
    public sealed class SortedSetRangeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetRangeResult"/> class.
        /// </summary>
        public SortedSetRangeResult(SortedSetResultStatus status, SortedSetEntry[] entries)
        {
            Status = status;
            Entries = entries;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SortedSetResultStatus Status { get; }
        /// <summary>
        /// Gets the entries.
        /// </summary>
        public SortedSetEntry[] Entries { get; }
    }

    /// <summary>
    /// Represents a sorted set score result.
    /// </summary>
    public sealed class SortedSetScoreResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetScoreResult"/> class.
        /// </summary>
        public SortedSetScoreResult(SortedSetResultStatus status, double? score)
        {
            Status = status;
            Score = score;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SortedSetResultStatus Status { get; }
        /// <summary>
        /// Gets the score, or null if member does not exist.
        /// </summary>
        public double? Score { get; }
    }

    /// <summary>
    /// Represents a sorted set rank result.
    /// </summary>
    public sealed class SortedSetRankResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetRankResult"/> class.
        /// </summary>
        public SortedSetRankResult(SortedSetResultStatus status, long? rank)
        {
            Status = status;
            Rank = rank;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SortedSetResultStatus Status { get; }
        /// <summary>
        /// Gets the rank (0-based position), or null if member does not exist.
        /// </summary>
        public long? Rank { get; }
    }

    /// <summary>
    /// Represents a sorted set remove range result.
    /// </summary>
    public sealed class SortedSetRemoveRangeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SortedSetRemoveRangeResult"/> class.
        /// </summary>
        public SortedSetRemoveRangeResult(SortedSetResultStatus status, long removed)
        {
            Status = status;
            Removed = removed;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public SortedSetResultStatus Status { get; }
        /// <summary>
        /// Gets the number of elements removed.
        /// </summary>
        public long Removed { get; }
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
    /// Describes results of HyperLogLog operations.
    /// </summary>
    public enum HyperLogLogResultStatus
    {
        Ok,
        WrongType
    }

    /// <summary>
    /// Represents a result for HyperLogLog add operations.
    /// </summary>
    public sealed class HyperLogLogAddResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HyperLogLogAddResult"/> class.
        /// </summary>
        public HyperLogLogAddResult(HyperLogLogResultStatus status, bool changed)
        {
            Status = status;
            Changed = changed;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public HyperLogLogResultStatus Status { get; }
        /// <summary>
        /// Gets whether at least one register changed.
        /// </summary>
        public bool Changed { get; }
    }

    /// <summary>
    /// Represents a result for HyperLogLog count operations.
    /// </summary>
    public sealed class HyperLogLogCountResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HyperLogLogCountResult"/> class.
        /// </summary>
        public HyperLogLogCountResult(HyperLogLogResultStatus status, long count)
        {
            Status = status;
            Count = count;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public HyperLogLogResultStatus Status { get; }
        /// <summary>
        /// Gets the approximate cardinality.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Represents a result for HyperLogLog merge operations.
    /// </summary>
    public sealed class HyperLogLogMergeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HyperLogLogMergeResult"/> class.
        /// </summary>
        public HyperLogLogMergeResult(HyperLogLogResultStatus status)
        {
            Status = status;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public HyperLogLogResultStatus Status { get; }
    }

    /// <summary>
    /// Describes common results for probabilistic sketch operations.
    /// </summary>
    public enum ProbabilisticResultStatus
    {
        Ok,
        WrongType,
        NotFound,
        Exists,
        InvalidArgument
    }

    /// <summary>
    /// Represents a generic count result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticCountResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticCountResult"/> class.
        /// </summary>
        public ProbabilisticCountResult(ProbabilisticResultStatus status, long count)
        {
            Status = status;
            Count = count;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets the count value.
        /// </summary>
        public long Count { get; }
    }

    /// <summary>
    /// Represents a generic boolean result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticBoolResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticBoolResult"/> class.
        /// </summary>
        public ProbabilisticBoolResult(ProbabilisticResultStatus status, bool value)
        {
            Status = status;
            Value = value;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets the boolean result value.
        /// </summary>
        public bool Value { get; }
    }

    /// <summary>
    /// Represents an array result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticArrayResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticArrayResult"/> class.
        /// </summary>
        public ProbabilisticArrayResult(ProbabilisticResultStatus status, long[] values)
        {
            Status = status;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets result values.
        /// </summary>
        public long[] Values { get; }
    }

    /// <summary>
    /// Represents a floating-point array result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticDoubleArrayResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticDoubleArrayResult"/> class.
        /// </summary>
        public ProbabilisticDoubleArrayResult(ProbabilisticResultStatus status, double[] values)
        {
            Status = status;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets result values.
        /// </summary>
        public double[] Values { get; }
    }

    /// <summary>
    /// Represents a scalar floating-point result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticDoubleResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticDoubleResult"/> class.
        /// </summary>
        public ProbabilisticDoubleResult(ProbabilisticResultStatus status, double? value)
        {
            Status = status;
            Value = value;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets result value.
        /// </summary>
        public double? Value { get; }
    }

    /// <summary>
    /// Represents string-array result for probabilistic operations.
    /// </summary>
    public sealed class ProbabilisticStringArrayResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticStringArrayResult"/> class.
        /// </summary>
        public ProbabilisticStringArrayResult(ProbabilisticResultStatus status, string?[] values)
        {
            Status = status;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets string results.
        /// </summary>
        public string?[] Values { get; }
    }

    /// <summary>
    /// Represents info result for probabilistic structures.
    /// </summary>
    public sealed class ProbabilisticInfoResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProbabilisticInfoResult"/> class.
        /// </summary>
        public ProbabilisticInfoResult(ProbabilisticResultStatus status, KeyValuePair<string, string>[] fields)
        {
            Status = status;
            Fields = fields;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public ProbabilisticResultStatus Status { get; }
        /// <summary>
        /// Gets key/value fields describing the sketch.
        /// </summary>
        public KeyValuePair<string, string>[] Fields { get; }
    }

    /// <summary>
    /// Describes results of vector operations.
    /// </summary>
    public enum VectorResultStatus
    {
        Ok,
        WrongType,
        NotFound,
        InvalidArgument
    }

    /// <summary>
    /// Represents a result for vector set operations.
    /// </summary>
    public sealed class VectorSetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorSetResult"/> class.
        /// </summary>
        public VectorSetResult(VectorResultStatus status)
        {
            Status = status;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
    }

    /// <summary>
    /// Represents a result for vector get operations.
    /// </summary>
    public sealed class VectorGetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorGetResult"/> class.
        /// </summary>
        public VectorGetResult(VectorResultStatus status, double[]? vector)
        {
            Status = status;
            Vector = vector;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
        /// <summary>
        /// Gets the vector value.
        /// </summary>
        public double[]? Vector { get; }
    }

    /// <summary>
    /// Represents a result for vector dimension operations.
    /// </summary>
    public sealed class VectorSizeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorSizeResult"/> class.
        /// </summary>
        public VectorSizeResult(VectorResultStatus status, long size)
        {
            Status = status;
            Size = size;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
        /// <summary>
        /// Gets the vector dimension.
        /// </summary>
        public long Size { get; }
    }

    /// <summary>
    /// Represents a result for vector similarity operations.
    /// </summary>
    public sealed class VectorSimilarityResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorSimilarityResult"/> class.
        /// </summary>
        public VectorSimilarityResult(VectorResultStatus status, double? value)
        {
            Status = status;
            Value = value;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
        /// <summary>
        /// Gets the similarity (or distance) value.
        /// </summary>
        public double? Value { get; }
    }

    /// <summary>
    /// Represents a result for vector delete operations.
    /// </summary>
    public sealed class VectorDeleteResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorDeleteResult"/> class.
        /// </summary>
        public VectorDeleteResult(VectorResultStatus status, long deleted)
        {
            Status = status;
            Deleted = deleted;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
        /// <summary>
        /// Gets the deleted vector count.
        /// </summary>
        public long Deleted { get; }
    }

    /// <summary>
    /// Represents a vector search entry.
    /// </summary>
    public sealed class VectorSearchEntry
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorSearchEntry"/> class.
        /// </summary>
        public VectorSearchEntry(string key, double score)
        {
            Key = key;
            Score = score;
        }

        /// <summary>
        /// Gets the vector key.
        /// </summary>
        public string Key { get; }
        /// <summary>
        /// Gets the vector score.
        /// </summary>
        public double Score { get; }
    }

    /// <summary>
    /// Represents a result for vector search operations.
    /// </summary>
    public sealed class VectorSearchResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VectorSearchResult"/> class.
        /// </summary>
        public VectorSearchResult(VectorResultStatus status, VectorSearchEntry[] entries)
        {
            Status = status;
            Entries = entries;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public VectorResultStatus Status { get; }
        /// <summary>
        /// Gets the search result entries.
        /// </summary>
        public VectorSearchEntry[] Entries { get; }
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
        /// Adds one or more members to a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="entries">The entries to add.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number added.</returns>
        Task<SortedSetCountResult> SortedSetAddAsync(
            string key,
            SortedSetEntry[] entries,
            CancellationToken token = default);

        /// <summary>
        /// Removes one or more members from a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="members">Members to remove.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number removed.</returns>
        Task<SortedSetCountResult> SortedSetRemoveAsync(
            string key,
            byte[][] members,
            CancellationToken token = default);

        /// <summary>
        /// Returns a range of members in a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="start">Start index.</param>
        /// <param name="stop">Stop index (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains entries.</returns>
        Task<SortedSetRangeResult> SortedSetRangeAsync(
            string key,
            int start,
            int stop,
            CancellationToken token = default);

        /// <summary>
        /// Returns the cardinality of a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the set count.</returns>
        Task<SortedSetCountResult> SortedSetCardinalityAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Returns members from a sorted set within a score range.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="minScore">Minimum score (inclusive).</param>
        /// <param name="maxScore">Maximum score (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains entries.</returns>
        Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(
            string key,
            double minScore,
            double maxScore,
            CancellationToken token = default);

        /// <summary>
        /// Returns the score of a member in a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="member">The member.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the score.</returns>
        Task<SortedSetScoreResult> SortedSetScoreAsync(
            string key,
            byte[] member,
            CancellationToken token = default);

        /// <summary>
        /// Increments the score of a member in a sorted set.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="increment">The value to add to the score.</param>
        /// <param name="member">The member.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new score.</returns>
        Task<SortedSetScoreResult> SortedSetIncrementAsync(
            string key,
            double increment,
            byte[] member,
            CancellationToken token = default);

        /// <summary>
        /// Counts members in a sorted set within a score range.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="minScore">Minimum score (inclusive).</param>
        /// <param name="maxScore">Maximum score (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the count.</returns>
        Task<SortedSetCountResult> SortedSetCountByScoreAsync(
            string key,
            double minScore,
            double maxScore,
            CancellationToken token = default);

        /// <summary>
        /// Returns the rank of a member in a sorted set (ascending order, 0-based).
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="member">The member.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the rank.</returns>
        Task<SortedSetRankResult> SortedSetRankAsync(
            string key,
            byte[] member,
            CancellationToken token = default);

        /// <summary>
        /// Returns the rank of a member in a sorted set (descending order, 0-based).
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="member">The member.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the rank.</returns>
        Task<SortedSetRankResult> SortedSetReverseRankAsync(
            string key,
            byte[] member,
            CancellationToken token = default);

        /// <summary>
        /// Removes members from a sorted set within a score range.
        /// </summary>
        /// <param name="key">The sorted set key.</param>
        /// <param name="minScore">Minimum score (inclusive).</param>
        /// <param name="maxScore">Maximum score (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the count removed.</returns>
        Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(
            string key,
            double minScore,
            double maxScore,
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

        /// <summary>
        /// Sets a JSON value at the specified key and path.
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath (e.g., "$", "$.users[0].name").</param>
        /// <param name="value">The JSON value as a byte array (UTF-8 encoded JSON).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the status.</returns>
        Task<JsonSetResult> JsonSetAsync(
            string key,
            string path,
            byte[] value,
            CancellationToken token = default);

        /// <summary>
        /// Gets a JSON value at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="paths">One or more JSONPaths.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the values.</returns>
        Task<JsonGetResult> JsonGetAsync(
            string key,
            string[] paths,
            CancellationToken token = default);

        /// <summary>
        /// Deletes values at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="paths">One or more JSONPaths.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the count deleted.</returns>
        Task<JsonDelResult> JsonDelAsync(
            string key,
            string[] paths,
            CancellationToken token = default);

        /// <summary>
        /// Gets the type of value at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="paths">One or more JSONPaths.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the types.</returns>
        Task<JsonTypeResult> JsonTypeAsync(
            string key,
            string[] paths,
            CancellationToken token = default);

        /// <summary>
        /// Gets string length at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="paths">One or more JSONPaths.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains lengths.</returns>
        Task<JsonArrayResult> JsonStrlenAsync(
            string key,
            string[] paths,
            CancellationToken token = default);

        /// <summary>
        /// Gets array length at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="paths">One or more JSONPaths.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains array lengths.</returns>
        Task<JsonArrayResult> JsonArrlenAsync(
            string key,
            string[] paths,
            CancellationToken token = default);

        /// <summary>
        /// Appends value(s) to arrays at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath to the array.</param>
        /// <param name="values">Values to append (each as UTF-8 encoded JSON).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new array length.</returns>
        Task<JsonArrayResult> JsonArrappendAsync(
            string key,
            string path,
            byte[][] values,
            CancellationToken token = default);

        /// <summary>
        /// Gets values from an array at the specified path(s).
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath to the array.</param>
        /// <param name="start">The start index.</param>
        /// <param name="end">The stop index (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the array values.</returns>
        Task<JsonGetResult> JsonArrindexAsync(
            string key,
            string path,
            byte[] value,
            CancellationToken token = default);

        /// <summary>
        /// Inserts value(s) at an index in arrays.
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath to the array.</param>
        /// <param name="index">The index to insert at.</param>
        /// <param name="values">Values to insert (each as UTF-8 encoded JSON).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new array length.</returns>
        Task<JsonArrayResult> JsonArrinsertAsync(
            string key,
            string path,
            int index,
            byte[][] values,
            CancellationToken token = default);

        /// <summary>
        /// Removes elements from arrays.
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath to the array.</param>
        /// <param name="index">The index to remove, or null to remove all occurrences of a value.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new array length.</returns>
        Task<JsonArrayResult> JsonArrremAsync(
            string key,
            string path,
            int? index,
            CancellationToken token = default);

        /// <summary>
        /// Trims arrays to a range.
        /// </summary>
        /// <param name="key">The JSON document key.</param>
        /// <param name="path">The JSONPath to the array.</param>
        /// <param name="start">The start index.</param>
        /// <param name="stop">The stop index (inclusive).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the new array length.</returns>
        Task<JsonArrayResult> JsonArrtrimAsync(
            string key,
            string path,
            int start,
            int stop,
            CancellationToken token = default);

        /// <summary>
        /// Gets multiple JSON documents by key.
        /// </summary>
        /// <param name="keys">The keys to retrieve.</param>
        /// <param name="path">The JSONPath for all keys.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the values.</returns>
        Task<JsonMGetResult> JsonMgetAsync(
            string[] keys,
            string path,
            CancellationToken token = default);

        /// <summary>
        /// Adds one or more elements to a HyperLogLog sketch.
        /// </summary>
        /// <param name="key">The HyperLogLog key.</param>
        /// <param name="elements">The elements to add.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status and whether the sketch changed.</returns>
        Task<HyperLogLogAddResult> HyperLogLogAddAsync(
            string key,
            byte[][] elements,
            CancellationToken token = default);

        /// <summary>
        /// Returns the approximate cardinality for one or more HyperLogLog sketches.
        /// </summary>
        /// <param name="keys">The HyperLogLog keys.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains status and approximate cardinality.</returns>
        Task<HyperLogLogCountResult> HyperLogLogCountAsync(
            string[] keys,
            CancellationToken token = default);

        /// <summary>
        /// Merges one or more HyperLogLog sketches into a destination sketch.
        /// </summary>
        /// <param name="destinationKey">The destination HyperLogLog key.</param>
        /// <param name="sourceKeys">The source HyperLogLog keys.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains merge status.</returns>
        Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(
            string destinationKey,
            string[] sourceKeys,
            CancellationToken token = default);

        /// <summary>
        /// Reserves a Bloom filter at key with error rate and capacity.
        /// </summary>
        Task<ProbabilisticResultStatus> BloomReserveAsync(
            string key,
            double errorRate,
            long capacity,
            CancellationToken token = default);

        /// <summary>
        /// Adds one element to a Bloom filter.
        /// </summary>
        Task<ProbabilisticBoolResult> BloomAddAsync(
            string key,
            byte[] element,
            CancellationToken token = default);

        /// <summary>
        /// Adds multiple elements to a Bloom filter.
        /// </summary>
        Task<ProbabilisticArrayResult> BloomMAddAsync(
            string key,
            byte[][] elements,
            CancellationToken token = default);

        /// <summary>
        /// Checks whether an element exists in a Bloom filter.
        /// </summary>
        Task<ProbabilisticBoolResult> BloomExistsAsync(
            string key,
            byte[] element,
            CancellationToken token = default);

        /// <summary>
        /// Checks whether multiple elements exist in a Bloom filter.
        /// </summary>
        Task<ProbabilisticArrayResult> BloomMExistsAsync(
            string key,
            byte[][] elements,
            CancellationToken token = default);

        /// <summary>
        /// Returns info fields for a Bloom filter.
        /// </summary>
        Task<ProbabilisticInfoResult> BloomInfoAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Reserves a Cuckoo filter at key with capacity.
        /// </summary>
        Task<ProbabilisticResultStatus> CuckooReserveAsync(
            string key,
            long capacity,
            CancellationToken token = default);

        /// <summary>
        /// Adds an item to a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticBoolResult> CuckooAddAsync(
            string key,
            byte[] item,
            bool noCreate,
            CancellationToken token = default);

        /// <summary>
        /// Adds an item only if it does not already exist in a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticBoolResult> CuckooAddNxAsync(
            string key,
            byte[] item,
            bool noCreate,
            CancellationToken token = default);

        /// <summary>
        /// Checks whether an item exists in a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticBoolResult> CuckooExistsAsync(
            string key,
            byte[] item,
            CancellationToken token = default);

        /// <summary>
        /// Deletes one occurrence of an item from a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticBoolResult> CuckooDeleteAsync(
            string key,
            byte[] item,
            CancellationToken token = default);

        /// <summary>
        /// Returns the count of an item in a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticCountResult> CuckooCountAsync(
            string key,
            byte[] item,
            CancellationToken token = default);

        /// <summary>
        /// Returns info fields for a Cuckoo filter.
        /// </summary>
        Task<ProbabilisticInfoResult> CuckooInfoAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Creates a t-digest sketch.
        /// </summary>
        Task<ProbabilisticResultStatus> TDigestCreateAsync(
            string key,
            int compression,
            CancellationToken token = default);

        /// <summary>
        /// Resets a t-digest sketch.
        /// </summary>
        Task<ProbabilisticResultStatus> TDigestResetAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Adds one or more values to a t-digest sketch.
        /// </summary>
        Task<ProbabilisticResultStatus> TDigestAddAsync(
            string key,
            double[] values,
            CancellationToken token = default);

        /// <summary>
        /// Returns quantiles for the provided fractions.
        /// </summary>
        Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(
            string key,
            double[] quantiles,
            CancellationToken token = default);

        /// <summary>
        /// Returns CDF values for the provided points.
        /// </summary>
        Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(
            string key,
            double[] values,
            CancellationToken token = default);

        /// <summary>
        /// Returns ranks (number of observations strictly less than each value).
        /// </summary>
        Task<ProbabilisticArrayResult> TDigestRankAsync(
            string key,
            double[] values,
            CancellationToken token = default);

        /// <summary>
        /// Returns reverse ranks (number of observations strictly greater than each value).
        /// </summary>
        Task<ProbabilisticArrayResult> TDigestRevRankAsync(
            string key,
            double[] values,
            CancellationToken token = default);

        /// <summary>
        /// Returns values by 0-based ranks.
        /// </summary>
        Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(
            string key,
            long[] ranks,
            CancellationToken token = default);

        /// <summary>
        /// Returns values by reverse 0-based ranks.
        /// </summary>
        Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(
            string key,
            long[] ranks,
            CancellationToken token = default);

        /// <summary>
        /// Returns mean of values between two quantile cutoffs.
        /// </summary>
        Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(
            string key,
            double lowerQuantile,
            double upperQuantile,
            CancellationToken token = default);

        /// <summary>
        /// Returns minimum value from a t-digest sketch.
        /// </summary>
        Task<ProbabilisticDoubleResult> TDigestMinAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Returns maximum value from a t-digest sketch.
        /// </summary>
        Task<ProbabilisticDoubleResult> TDigestMaxAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Returns info fields for a t-digest sketch.
        /// </summary>
        Task<ProbabilisticInfoResult> TDigestInfoAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Reserves a Top-K sketch.
        /// </summary>
        Task<ProbabilisticResultStatus> TopKReserveAsync(
            string key,
            int k,
            int width,
            int depth,
            double decay,
            CancellationToken token = default);

        /// <summary>
        /// Adds one or more items to a Top-K sketch and returns dropped items.
        /// </summary>
        Task<ProbabilisticStringArrayResult> TopKAddAsync(
            string key,
            byte[][] items,
            CancellationToken token = default);

        /// <summary>
        /// Increments one or more items in a Top-K sketch and returns dropped items.
        /// </summary>
        Task<ProbabilisticStringArrayResult> TopKIncrByAsync(
            string key,
            KeyValuePair<byte[], long>[] increments,
            CancellationToken token = default);

        /// <summary>
        /// Queries whether items are currently in Top-K.
        /// </summary>
        Task<ProbabilisticArrayResult> TopKQueryAsync(
            string key,
            byte[][] items,
            CancellationToken token = default);

        /// <summary>
        /// Returns estimated counts for items in Top-K.
        /// </summary>
        Task<ProbabilisticArrayResult> TopKCountAsync(
            string key,
            byte[][] items,
            CancellationToken token = default);

        /// <summary>
        /// Lists items currently in Top-K, with optional counts.
        /// </summary>
        Task<ProbabilisticStringArrayResult> TopKListAsync(
            string key,
            bool withCount,
            CancellationToken token = default);

        /// <summary>
        /// Returns info fields for a Top-K sketch.
        /// </summary>
        Task<ProbabilisticInfoResult> TopKInfoAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Sets a vector value at the specified key.
        /// </summary>
        /// <param name="key">The vector key.</param>
        /// <param name="vector">The vector components.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the status.</returns>
        Task<VectorSetResult> VectorSetAsync(
            string key,
            double[] vector,
            CancellationToken token = default);

        /// <summary>
        /// Gets a vector value at the specified key.
        /// </summary>
        /// <param name="key">The vector key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the vector.</returns>
        Task<VectorGetResult> VectorGetAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Gets vector dimension for the specified key.
        /// </summary>
        /// <param name="key">The vector key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the vector size.</returns>
        Task<VectorSizeResult> VectorSizeAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Computes similarity (or distance) between two vectors.
        /// </summary>
        /// <param name="key">The first vector key.</param>
        /// <param name="otherKey">The second vector key.</param>
        /// <param name="metric">The metric name (COSINE, DOT, or L2).</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the computed value.</returns>
        Task<VectorSimilarityResult> VectorSimilarityAsync(
            string key,
            string otherKey,
            string metric,
            CancellationToken token = default);

        /// <summary>
        /// Deletes a vector key.
        /// </summary>
        /// <param name="key">The vector key.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains delete status and count.</returns>
        Task<VectorDeleteResult> VectorDeleteAsync(
            string key,
            CancellationToken token = default);

        /// <summary>
        /// Searches vectors by prefix and returns top-k scored matches.
        /// </summary>
        /// <param name="keyPrefix">The key prefix to search (ordinal starts-with).</param>
        /// <param name="topK">The maximum number of results to return.</param>
        /// <param name="offset">The number of leading matches to skip for paging.</param>
        /// <param name="metric">The metric name (COSINE, DOT, or L2).</param>
        /// <param name="queryVector">The query vector.</param>
        /// <param name="token">A cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains scored entries.</returns>
        Task<VectorSearchResult> VectorSearchAsync(
            string keyPrefix,
            int topK,
            int offset,
            string metric,
            double[] queryVector,
            CancellationToken token = default);
    }
}

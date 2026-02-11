using System.Collections.Generic;

namespace Dredis
{
    public enum SetCondition
    {
        None,
        Nx,
        Xx
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

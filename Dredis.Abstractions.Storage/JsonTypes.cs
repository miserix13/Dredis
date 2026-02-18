using System;
using System.Collections.Generic;
using System.Text.Json;

namespace Dredis.Abstractions.Storage
{
    /// <summary>
    /// Describes results of JSON operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These status codes are returned by JSON operations to indicate success or various error conditions:
    /// <list type="bullet">
    /// <item><description><see cref="Ok"/> - Operation completed successfully</description></item>
    /// <item><description><see cref="WrongType"/> - Key exists but contains a non-JSON value</description></item>
    /// <item><description><see cref="PathNotFound"/> - Specified JSONPath does not exist in the document</description></item>
    /// <item><description><see cref="InvalidPath"/> - JSONPath syntax is malformed</description></item>
    /// <item><description><see cref="InvalidJson"/> - Provided JSON data is malformed and cannot be parsed</description></item>
    /// <item><description><see cref="PathExists"/> - Path already exists (for operations requiring non-existent paths)</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public enum JsonResultStatus
    {
        /// <summary>
        /// Operation completed successfully.
        /// </summary>
        Ok,
        
        /// <summary>
        /// Key exists but is not a JSON document type.
        /// </summary>
        WrongType,
        
        /// <summary>
        /// The specified JSONPath does not exist in the document.
        /// </summary>
        PathNotFound,
        
        /// <summary>
        /// The JSONPath syntax is invalid.
        /// </summary>
        InvalidPath,
        
        /// <summary>
        /// The JSON data is malformed and cannot be parsed.
        /// </summary>
        InvalidJson,
        
        /// <summary>
        /// The path already exists (for operations requiring non-existent paths).
        /// </summary>
        PathExists
    }

    /// <summary>
    /// Represents a JSON GET result.
    /// </summary>
    public sealed class JsonGetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonGetResult"/> class.
        /// </summary>
        public JsonGetResult(JsonResultStatus status, byte[]? value = null, byte[][]? values = null)
        {
            Status = status;
            Value = value;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets the value for single path queries, or null if multiple paths or error.
        /// </summary>
        public byte[]? Value { get; }

        /// <summary>
        /// Gets the values for multi-path queries, or null if single path or error.
        /// </summary>
        public byte[][]? Values { get; }
    }

    /// <summary>
    /// Represents a JSON SET result.
    /// </summary>
    public sealed class JsonSetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSetResult"/> class.
        /// </summary>
        public JsonSetResult(JsonResultStatus status, bool created = false)
        {
            Status = status;
            Created = created;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets a value indicating whether a new key was created.
        /// </summary>
        public bool Created { get; }
    }

    /// <summary>
    /// Represents a JSON DEL result.
    /// </summary>
    public sealed class JsonDelResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDelResult"/> class.
        /// </summary>
        public JsonDelResult(JsonResultStatus status, long deleted = 0)
        {
            Status = status;
            Deleted = deleted;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets the number of paths deleted.
        /// </summary>
        public long Deleted { get; }
    }

    /// <summary>
    /// Represents a JSON TYPE result.
    /// </summary>
    public sealed class JsonTypeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonTypeResult"/> class.
        /// </summary>
        public JsonTypeResult(JsonResultStatus status, string[]? types = null)
        {
            Status = status;
            Types = types;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets the JSON types (one per path).
        /// </summary>
        public string[]? Types { get; }
    }

    /// <summary>
    /// Represents a JSON array operation result with count.
    /// </summary>
    public sealed class JsonArrayResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonArrayResult"/> class.
        /// </summary>
        public JsonArrayResult(JsonResultStatus status, long? count = null, long[]? counts = null)
        {
            Status = status;
            Count = count;
            Counts = counts;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets the count for single path, or null for multi-path.
        /// </summary>
        public long? Count { get; }

        /// <summary>
        /// Gets the counts for multi-path queries, or null for single path.
        /// </summary>
        public long[]? Counts { get; }
    }

    /// <summary>
    /// Represents a JSON MGET result.
    /// </summary>
    public sealed class JsonMGetResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonMGetResult"/> class.
        /// </summary>
        public JsonMGetResult(JsonResultStatus status, byte[][]? values = null)
        {
            Status = status;
            Values = values;
        }

        /// <summary>
        /// Gets the result status.
        /// </summary>
        public JsonResultStatus Status { get; }

        /// <summary>
        /// Gets the values retrieved (one per key).
        /// </summary>
        public byte[][]? Values { get; }
    }

    /// <summary>
    /// Represents a simple JSON wrapper for manipulation.
    /// </summary>
    public sealed class JsonWrapper
    {
        private JsonElement _root;

        public JsonWrapper(JsonElement root)
        {
            _root = root;
        }

        public static JsonWrapper? TryParse(byte[] data)
        {
            try
            {
                var element = JsonSerializer.Deserialize<JsonElement>(data);
                return new JsonWrapper(element);
            }
            catch
            {
                return null;
            }
        }

        public JsonElement Root => _root;

        public byte[] ToBytes()
        {
            return JsonSerializer.SerializeToUtf8Bytes(_root);
        }

        public JsonElement? GetAtPath(string path)
        {
            if (path == "$" || path == "")
            {
                return _root;
            }

            var parts = ParsePath(path);
            var current = _root;

            foreach (var part in parts)
            {
                if (part.IsIndex)
                {
                    if (current.ValueKind != JsonValueKind.Array)
                    {
                        return null;
                    }

                    if (part.Index < 0 || part.Index >= current.GetArrayLength())
                    {
                        return null;
                    }

                    current = current[part.Index];
                }
                else
                {
                    if (current.ValueKind != JsonValueKind.Object)
                    {
                        return null;
                    }

                    if (!current.TryGetProperty(part.Key!, out var result))
                    {
                        return null;
                    }

                    current = result;
                }
            }

            return current;
        }

        public List<JsonElement> GetAtPaths(string path)
        {
            var results = new List<JsonElement>();

            if (path == "$" || path == "")
            {
                results.Add(_root);
                return results;
            }

            var parts = ParsePath(path);
            var candidates = new List<(JsonElement element, int index)> { (_root, 0) };

            foreach (var part in parts)
            {
                var newCandidates = new List<(JsonElement element, int index)>();

                foreach (var (current, depth) in candidates)
                {
                    if (part.IsIndex)
                    {
                        if (current.ValueKind == JsonValueKind.Array)
                        {
                            var index = part.Index < 0 
                                ? current.GetArrayLength() + part.Index 
                                : part.Index;

                            if (index >= 0 && index < current.GetArrayLength())
                            {
                                newCandidates.Add((current[index], depth + 1));
                            }
                        }
                    }
                    else if (part.IsWildcard)
                    {
                        if (current.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in current.EnumerateObject())
                            {
                                newCandidates.Add((prop.Value, depth + 1));
                            }
                        }
                        else if (current.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var item in current.EnumerateArray())
                            {
                                newCandidates.Add((item, depth + 1));
                            }
                        }
                    }
                    else
                    {
                        if (current.ValueKind == JsonValueKind.Object && 
                            current.TryGetProperty(part.Key!, out var result))
                        {
                            newCandidates.Add((result, depth + 1));
                        }
                    }
                }

                candidates = newCandidates;

                if (candidates.Count == 0)
                {
                    break;
                }
            }

            foreach (var (element, _) in candidates)
            {
                results.Add(element);
            }

            return results;
        }

        private List<PathPart> ParsePath(string path)
        {
            var parts = new List<PathPart>();

            if (!path.StartsWith("$"))
            {
                throw new InvalidOperationException("Path must start with $");
            }

            var current = 1;
            while (current < path.Length)
            {
                if (path[current] == '.')
                {
                    current++;
                    if (current >= path.Length)
                    {
                        break;
                    }

                    if (path[current] == '*')
                    {
                        parts.Add(new PathPart { IsWildcard = true });
                        current++;
                    }
                    else
                    {
                        var keyStart = current;
                        while (current < path.Length && path[current] != '.' && path[current] != '[')
                        {
                            current++;
                        }

                        parts.Add(new PathPart { Key = path.Substring(keyStart, current - keyStart) });
                    }
                }
                else if (path[current] == '[')
                {
                    current++;
                    var indexStart = current;
                    while (current < path.Length && path[current] != ']')
                    {
                        current++;
                    }

                    var indexStr = path.Substring(indexStart, current - indexStart);
                    if (indexStr == "*")
                    {
                        parts.Add(new PathPart { IsWildcard = true });
                    }
                    else if (int.TryParse(indexStr, out var index))
                    {
                        parts.Add(new PathPart { IsIndex = true, Index = index });
                    }

                    current++; // skip ]
                }
                else
                {
                    current++;
                }
            }

            return parts;
        }

        private sealed class PathPart
        {
            public string? Key { get; set; }
            public bool IsIndex { get; set; }
            public bool IsWildcard { get; set; }
            public int Index { get; set; }
        }
    }
}

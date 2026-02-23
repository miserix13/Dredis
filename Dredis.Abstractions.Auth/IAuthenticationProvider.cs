namespace Dredis.Abstractions.Auth
{
    /// <summary>
    /// Represents credentials supplied by a client for authentication.
    /// </summary>
    /// <param name="Username">The user name for identity-based authentication.</param>
    /// <param name="Password">The password for identity-based authentication.</param>
    /// <param name="Token">An optional bearer token or API key for token-based authentication.</param>
    public sealed record AuthenticationRequest(string? Username, string? Password, string? Token);

    /// <summary>
    /// Represents the authenticated principal returned by an authentication provider.
    /// </summary>
    /// <param name="SubjectId">A stable identifier for the authenticated subject.</param>
    /// <param name="DisplayName">An optional display name for the subject.</param>
    /// <param name="Claims">Optional key/value claims describing the subject.</param>
    public sealed record AuthenticatedIdentity(
        string SubjectId,
        string? DisplayName,
        IReadOnlyDictionary<string, string>? Claims = null);

    /// <summary>
    /// Defines authentication behavior for validating incoming credentials.
    /// </summary>
    public interface IAuthenticationProvider
    {
        /// <summary>
        /// Validates the provided credentials and returns an authenticated identity when successful.
        /// </summary>
        /// <param name="request">The authentication request details.</param>
        /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
        /// <returns>
        /// A task that resolves to an <see cref="AuthenticatedIdentity"/> when authentication succeeds;
        /// otherwise <see langword="null"/>.
        /// </returns>
        Task<AuthenticatedIdentity?> AuthenticateAsync(AuthenticationRequest request, CancellationToken cancellationToken = default);
    }
}

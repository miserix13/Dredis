namespace Dredis.Abstractions.Auth
{
    /// <summary>
    /// Defines a requested authorization check for an authenticated identity.
    /// </summary>
    /// <param name="Identity">The authenticated identity requesting access.</param>
    /// <param name="Action">The requested action (for example, a command name).</param>
    /// <param name="Resource">The target resource, if applicable.</param>
    public sealed record AuthorizationRequest(AuthenticatedIdentity Identity, string Action, string? Resource = null);

    /// <summary>
    /// Represents the outcome of an authorization decision.
    /// </summary>
    public enum AuthorizationDecision
    {
        Deny = 0,
        Allow = 1,
    }

    /// <summary>
    /// Defines authorization behavior for validating whether an action is allowed.
    /// </summary>
    public interface IAuthorizationProvider
    {
        /// <summary>
        /// Evaluates whether the provided authorization request is allowed.
        /// </summary>
        /// <param name="request">The authorization request to evaluate.</param>
        /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
        /// <returns>A task that resolves to an <see cref="AuthorizationDecision"/> value.</returns>
        Task<AuthorizationDecision> AuthorizeAsync(AuthorizationRequest request, CancellationToken cancellationToken = default);
    }
}

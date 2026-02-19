namespace Dredis.Abstractions.Command
{
    /// <summary>
    /// Represents a command that can be executed and is identified by a unique name.
    /// </summary>
    /// <remarks>Implementations of this interface should provide the logic for executing the command
    /// associated with the specified name. The Name property is used to distinguish between different commands and may
    /// be used for command lookup or registration.</remarks>
    public interface ICommand
    {
        /// <summary>
        /// Gets the name of the Command.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Executes an asynchronous operation using the specified parameters and returns the result as a string.
        /// </summary>
        /// <remarks>Ensure that the provided parameters meet any requirements defined by the
        /// implementation. Invalid or improperly formatted parameters may result in an error or unexpected
        /// behavior.</remarks>
        /// <param name="parameters">An array of strings representing the parameters to use for the operation. The meaning and required format of
        /// each parameter depend on the specific implementation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the output of the operation as a
        /// string.</returns>
        Task<string> ExecuteAsync(params string[] parameters);
    }
}

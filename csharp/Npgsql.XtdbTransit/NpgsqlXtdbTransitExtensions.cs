using Npgsql.XtdbTransit.Internal;

namespace Npgsql.XtdbTransit;

/// <summary>
/// Extension methods to enable XTDB transit-JSON support in Npgsql.
/// </summary>
public static class NpgsqlXtdbTransitExtensions
{
    /// <summary>
    /// Enables support for XTDB transit-JSON type (OID 16384).
    /// Call this on NpgsqlDataSourceBuilder before building the data source.
    /// This also registers a custom database info factory to work around XTDB's limited SQL support.
    /// </summary>
    /// <param name="builder">The NpgsqlDataSourceBuilder to configure.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// var builder = new NpgsqlDataSourceBuilder(connectionString);
    /// builder.UseTransit();
    /// var dataSource = builder.Build();
    /// </code>
    /// </example>
    public static NpgsqlDataSourceBuilder UseTransit(this NpgsqlDataSourceBuilder builder)
    {
        // Add type info resolver for transit type (OID 16384)
        builder.AddTypeInfoResolverFactory(new TransitTypeInfoResolverFactory());
        return builder;
    }
}

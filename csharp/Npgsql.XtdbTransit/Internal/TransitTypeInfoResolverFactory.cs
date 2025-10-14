using Npgsql.Internal;

namespace Npgsql.XtdbTransit.Internal;

/// <summary>
/// Factory for creating transit type info resolvers.
/// This is registered with NpgsqlDataSourceBuilder to enable transit support.
/// </summary>
internal sealed class TransitTypeInfoResolverFactory : PgTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver CreateResolver() => new TransitTypeInfoResolver();

    public override IPgTypeInfoResolver? CreateArrayResolver() => null; // No array support needed
}

using System;
using System.Reflection;
using Npgsql.Internal;
using Npgsql.Internal.Postgres;

namespace Npgsql.XtdbTransit.Internal;

/// <summary>
/// Type info resolver for XTDB transit-JSON type.
/// Maps string type to transit OID 16384.
/// </summary>
internal sealed class TransitTypeInfoResolver : IPgTypeInfoResolver
{
    private const uint TransitOid = 16384;

    public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
    {
        // Only handle explicit transit data type name requests
        if (type == typeof(string) && dataTypeName?.UnqualifiedName == "transit")
        {
            // Get the built-in text converter by asking for text type explicitly
            // Use GetTypeInfo with "text" to avoid recursion
            var textTypeInfo = options.GetTypeInfo(typeof(string), new PgTypeId(25)); // OID 25 = text
            if (textTypeInfo == null)
                throw new InvalidOperationException("Could not get text type info for OID 25");

            // Extract the converter using reflection
            var converterProperty = typeof(PgTypeInfo).GetProperty("Converter", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (converterProperty == null)
                throw new InvalidOperationException("Could not find Converter property on PgTypeInfo");

            var converter = converterProperty.GetValue(textTypeInfo) as PgConverter;
            if (converter == null)
                throw new InvalidOperationException("Could not get converter from text type info");

            // Create PgTypeInfo with transit OID (16384) and transit data type name
            // Use the pg_catalog.transit fully qualified name
            var transitDataTypeName = new DataTypeName("pg_catalog.transit");
            var typeInfo = new PgTypeInfo(options, converter, transitDataTypeName);

            // Set the PgTypeId (OID) to 16384 using reflection
            var pgTypeIdProperty = typeof(PgTypeInfo).GetProperty("PgTypeId", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (pgTypeIdProperty != null && pgTypeIdProperty.CanWrite)
            {
                pgTypeIdProperty.SetValue(typeInfo, new PgTypeId(TransitOid));
            }

            return typeInfo;
        }

        return null;
    }
}

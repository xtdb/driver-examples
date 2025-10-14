# Npgsql Custom Type Support Research

## Summary

Research into how Npgsql handles custom PostgreSQL types, domain types, and how to implement support for transit-JSON (OID 16384).

## Key Findings

### 0. Transit and other custom types are blocked by https://github.com/xtdb/xtdb/issues/4421

### 1. Type Mapping Approaches in Npgsql

Npgsql provides several ways to handle custom types:

#### A. Built-in Type Mapping (JSON/JSONB)
The simplest approach for JSON data:
```csharp
// Use NpgsqlDbType.Json for OID 114 (json)
// Use NpgsqlDbType.Jsonb for OID 3802 (jsonb)
cmd.Parameters.AddWithValue("@p1", NpgsqlDbType.Json, jsonString);
```

**Status**: ✅ Working - Successfully demonstrated in `TestLoadSampleJson` and `TestJsonNestOneFullRecord`

#### B. Composite Type Mapping
For mapping PostgreSQL composite types to C# classes:
```csharp
public class InventoryItem
{
    public string Name { get; set; }
    public int SupplierId { get; set; }
    public decimal Price { get; set; }
}

var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
dataSourceBuilder.MapComposite<InventoryItem>();
var dataSource = dataSourceBuilder.Build();
```

**Limitations**: Works for composite types but not for domain types with custom OIDs.

#### C. Custom Type Handlers (Advanced)
For completely custom types with specific OIDs:
```csharp
var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
dataSourceBuilder.AddTypeInfoResolverFactory(new MyCustomTypeResolverFactory());
var dataSource = dataSourceBuilder.Build();
```

**Requirements**: Must implement `PgTypeInfoResolverFactory` interface.

### 2. Domain Types vs Composite Types

**Domain Types** (like transit):
- Base types with constraints
- Have custom OIDs
- Cannot use standard MapComposite approach
- Require type handler implementation

**Composite Types**:
- Structured types with multiple fields
- Can be mapped directly with MapComposite

### 3. Custom Type Handler Implementation

To support custom OIDs like transit (16384), you need to:

1. **Create a Type Info Resolver Factory**
```csharp
public class TransitTypeInfoResolverFactory : PgTypeInfoResolverFactory
{
    public override IPgTypeInfoResolver CreateResolver() => new TransitTypeInfoResolver();
    public override IPgTypeInfoResolver? CreateArrayResolver() => null;
}
```

2. **Implement the Type Info Resolver**
```csharp
public class TransitTypeInfoResolver : IPgTypeInfoResolver
{
    public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
    {
        // Map string to transit OID 16384
        if (type == typeof(string) && dataTypeName?.UnqualifiedName == "transit")
        {
            return new PgTypeInfo(
                options,
                new TransitConverter(),
                new Oid(16384)
            );
        }
        return null;
    }
}
```

3. **Implement the Converter**
```csharp
public class TransitConverter : PgBufferedConverter<string>
{
    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        bufferRequirements = BufferRequirements.CreateFixedSize(1);
        return format is DataFormat.Text;
    }

    protected override string ReadCore(PgReader reader)
    {
        return reader.ReadString();
    }

    protected override void WriteCore(PgWriter writer, string value)
    {
        writer.WriteString(value);
    }

    public override Size GetSize(SizeContext context, string value, ref object? writeState)
    {
        return Size.Create(Encoding.UTF8.GetByteCount(value));
    }
}
```

4. **Register the Factory**
```csharp
var builder = new NpgsqlDataSourceBuilder(connectionString);
builder.AddTypeInfoResolverFactory(new TransitTypeInfoResolverFactory());
var dataSource = builder.Build();
```

### 4. Reference Implementations

**NodaTime Plugin** - Good reference for custom type handlers:
- Location: `https://github.com/npgsql/npgsql/tree/main/src/Npgsql.NodaTime`
- Shows how to implement `PgTypeInfoResolverFactory`
- Demonstrates date/time type mapping

**NetTopologySuite Plugin** - Another reference:
- Location: `https://github.com/npgsql/npgsql/tree/main/src/Npgsql.NetTopologySuite`
- Shows spatial type handling

### 5. API Evolution

**Important Note**: The custom type handler API changed significantly in Npgsql 8.0:
- Old: `TypeHandlerResolverFactory` and `TypeHandlerResolver`
- New: `PgTypeInfoResolverFactory` and `IPgTypeInfoResolver`

The new API is designed for NativeAOT compatibility.

### 6. Current Transit Support Status

**OID 114 (JSON)**: ✅ Fully working
- Use `NpgsqlDbType.Json` for parameters
- Successfully tested with nested data

**OID 16384 (Transit)**: ⚠️ Requires custom handler
- Would need `PgTypeInfoResolverFactory` implementation
- API marked as experimental
- May be complex for this use case

## Recommendations

### For the Driver Examples Project

1. **Keep JSON Support** (OID 114) - Already working well
   - `TestLoadSampleJson` ✅
   - `TestJsonNestOneFullRecord` ✅

2. **Skip Transit Custom Handler** (OID 16384) for now
   - Complex implementation required
   - API still experimental
   - Limited value-add over JSON support
   - Mark `TestParseTransitJson` as skipped with explanation

3. **Alternative: Document Transit Workaround**
   ```csharp
   // Transit can be handled as text and parsed manually if needed
   cmd.Parameters.AddWithValue("@p1", transitJsonString); // sends as text
   // XTDB may still accept it if it can infer the type
   ```

### If Transit Support is Required

Create a separate plugin project:
```
Npgsql.XtdbTransit/
├── TransitTypeInfoResolverFactory.cs
├── TransitTypeInfoResolver.cs
├── TransitConverter.cs
└── NpgsqlXtdbTransitExtensions.cs (with UseTransit() method)
```

This would follow the same pattern as `Npgsql.NodaTime`.

## Useful Links

- **Npgsql Type System Docs**: https://www.npgsql.org/dev/types.html
- **Composite Types**: https://www.npgsql.org/doc/types/enums_and_composites.html
- **Type Mapping**: https://www.npgsql.org/doc/types/basic.html
- **NpgsqlDataSourceBuilder API**: https://www.npgsql.org/doc/api/Npgsql.NpgsqlDataSourceBuilder.html
- **Npgsql GitHub**: https://github.com/npgsql/npgsql
- **Custom Type Handler Discussion**: https://github.com/npgsql/npgsql/issues/1550

## Conclusion

While Npgsql *can* support custom OID types like transit through `PgTypeInfoResolverFactory`, the implementation is non-trivial and the API is still experimental. For the driver examples project, the existing JSON support (OID 114) provides full functionality for demonstrating nested data, NEST_ONE, and type handling without requiring custom type handlers.

The transit-specific tests should remain skipped with a note explaining that transit OID support would require a custom type handler plugin implementation.

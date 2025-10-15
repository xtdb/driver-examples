<?php

namespace Xtdb\Example;

/**
 * Minimal Transit-JSON encoder/decoder for PHP
 * Supports basic transit format for XTDB integration
 */
class Transit
{
    /**
     * Encode a PHP associative array to transit-JSON map format
     * Format: ["^ ","~:key1",value1,"~:key2",value2,...]
     */
    public static function encodeMap(array $data): string
    {
        $pairs = [];
        foreach ($data as $key => $value) {
            $pairs[] = '"~:' . $key . '"';
            $pairs[] = self::encodeValue($value);
        }
        return '["^ ",' . implode(',', $pairs) . ']';
    }

    /**
     * Encode a single value to transit-JSON format
     */
    public static function encodeValue(mixed $value): string
    {
        return match (true) {
            is_string($value) => json_encode($value),
            is_bool($value) => $value ? 'true' : 'false',
            is_int($value) || is_float($value) => (string)$value,
            is_array($value) => self::encodeArray($value),
            $value instanceof \DateTime => '"~t' . $value->format('c') . '"',
            is_null($value) => 'null',
            default => json_encode((string)$value)
        };
    }

    /**
     * Encode a PHP array to transit-JSON format
     */
    private static function encodeArray(array $value): string
    {
        // Check if it's an associative array (map) or indexed array
        if (array_keys($value) === range(0, count($value) - 1)) {
            // Indexed array
            $encoded = array_map([self::class, 'encodeValue'], $value);
            return '[' . implode(',', $encoded) . ']';
        } else {
            // Associative array (map)
            return self::encodeMap($value);
        }
    }

    /**
     * Decode a transit-JSON string to PHP data structures
     * If value is already an array, return it as-is (already decoded by ext-pq)
     */
    public static function decode(string|array $value): mixed
    {
        // If already an array, it's been decoded by ext-pq - return as-is
        if (is_array($value)) {
            return $value;
        }

        // Try to parse as JSON first
        $data = json_decode($value, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            // Not JSON, check for PostgreSQL array format
            if (str_starts_with($value, '{') && str_ends_with($value, '}')) {
                return self::parsePgArray($value);
            }
            return $value;
        }

        return self::decodeValue($data);
    }

    /**
     * Decode a transit-JSON value recursively
     */
    private static function decodeValue(mixed $data): mixed
    {
        if (!is_array($data)) {
            // Check for transit string markers
            if (is_string($data)) {
                if (str_starts_with($data, '~:')) {
                    // Keyword - remove prefix
                    return substr($data, 2);
                } elseif (str_starts_with($data, '~t')) {
                    // Date - remove prefix
                    return substr($data, 2);
                }
            }
            return $data;
        }

        // Check for transit map: ["^ ", ...]
        if (isset($data[0]) && $data[0] === '^ ') {
            return self::decodeMap($data);
        }

        // Check for transit tagged value: ["~#tag", value]
        if (count($data) === 2 && isset($data[0]) && is_string($data[0]) && str_starts_with($data[0], '~#')) {
            // Tagged value - extract the actual value
            return self::decodeValue($data[1]);
        }

        // Regular array - decode each element
        $result = [];
        foreach ($data as $key => $value) {
            $result[$key] = self::decodeValue($value);
        }
        return $result;
    }

    /**
     * Decode a transit map to PHP associative array
     */
    private static function decodeMap(array $data): array
    {
        $result = [];
        $i = 1; // Start after "^ " marker

        while ($i < count($data)) {
            $key = self::decodeValue($data[$i]);
            $value = self::decodeValue($data[$i + 1]);
            $result[$key] = $value;
            $i += 2;
        }

        return $result;
    }

    /**
     * Parse PostgreSQL array format: {val1,val2} to PHP array
     */
    private static function parsePgArray(string $str): array
    {
        $content = substr($str, 1, -1);
        if (empty($content)) {
            return [];
        }

        // Split by comma and strip quotes from each element
        return array_map(
            fn($v) => trim($v, '"'),
            explode(',', $content)
        );
    }
}

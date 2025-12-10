#!/usr/bin/env python3
"""
Generate the XTDB feature compatibility matrix from test output.

This script runs all language tests and collects feature support information,
then generates a markdown table that can be copied into the README.

Usage:
    python scripts/generate-feature-matrix.py
"""

import subprocess
import sys
import re
from collections import defaultdict

# Feature list (in order of appearance in matrix)
FEATURES = [
    "Transit-Msgpack (COPY)",
    "NEST_ONE",
    "Arrow Flight SQL"
]

# Language order
LANGUAGES = [
    "Python",
    "Node.js",
    "Go",
    "Ruby",
    "Java",
    "Kotlin",
    "C",
    "C#",
    "Clojure",
    "Elixir",
    "Babashka",
    "PHP"
]

# Map test output language names to display names
LANGUAGE_MAP = {
    "python": "Python",
    "nodejs": "Node.js",
    "node": "Node.js",
    "go": "Go",
    "ruby": "Ruby",
    "java": "Java",
    "kotlin": "Kotlin",
    "c": "C",
    "csharp": "C#",
    "clojure": "Clojure",
    "elixir": "Elixir",
    "babashka": "Babashka",
    "php": "PHP"
}

def run_tests():
    """Run all language tests and capture output."""
    print("Running all language tests...")
    print("=" * 60)

    try:
        result = subprocess.run(
            ["mise", "run", "test:all"],
            capture_output=True,
            text=True,
            timeout=600,
            cwd="/workspaces/driver-examples"
        )

        output = result.stdout + result.stderr
        print("Test run complete!")
        print("=" * 60)
        return output

    except subprocess.TimeoutExpired:
        print("ERROR: Test run timed out after 10 minutes")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to run tests: {e}")
        sys.exit(1)

def parse_feature_reports(output):
    """Parse XTDB_FEATURE_UNSUPPORTED lines from test output."""
    # Initialize: all languages support all features by default
    features = defaultdict(lambda: {
        "Transit-Msgpack (COPY)": True,
        "NEST_ONE": True,
        "Arrow Flight SQL": True
    })

    # Initialize all languages
    for lang in LANGUAGES:
        features[lang]  # Access to create entry with defaults

    # Find unsupported feature declarations
    pattern = r'XTDB_FEATURE_UNSUPPORTED:\s+language=(\w+)\s+feature=([\w-]+)'

    for match in re.finditer(pattern, output):
        lang = match.group(1)
        feature = match.group(2)

        # Map to display name
        display_name = LANGUAGE_MAP.get(lang.lower(), lang)

        # Map feature name to matrix column name
        if feature == "transit-msgpack":
            features[display_name]["Transit-Msgpack (COPY)"] = False
        elif feature == "nest-one":
            features[display_name]["NEST_ONE"] = False
        elif feature == "arrow-flight-sql":
            features[display_name]["Arrow Flight SQL"] = False

    return dict(features)

def generate_matrix(features):
    """Generate markdown table from feature data."""

    if not features:
        print("\n⚠️  No languages found!")
        print("This shouldn't happen - check the test output parsing logic.")
        return None

    # Determine which features have at least one "no" (should be shown)
    features_to_show = []
    for feature in FEATURES:
        has_unsupported = any(
            not features.get(lang, {}).get(feature, True)
            for lang in LANGUAGES
        )
        if has_unsupported:
            features_to_show.append(feature)

    if not features_to_show:
        return "## All features are supported in all languages! 🎉\n"

    # Build markdown table
    lines = []
    lines.append("## Language Feature Compatibility Matrix")
    lines.append("")
    lines.append("The following matrix shows features that are not supported in some languages:")
    lines.append("")

    # Header
    header = "| Language | " + " | ".join(features_to_show) + " |"
    separator = "|----------|" + "|".join(["-" * (len(f) + 2) for f in features_to_show]) + "|"

    lines.append(header)
    lines.append(separator)

    # Rows
    for lang in LANGUAGES:
        lang_features = features.get(lang, {})
        cells = [f"**{lang}**"]

        for feature in features_to_show:
            supported = lang_features.get(feature, False)
            cells.append("✅" if supported else "❌")

        lines.append("| " + " | ".join(cells) + " |")

    lines.append("")
    lines.append("All languages support: Basic SQL, JSON, and Transit-JSON.")
    lines.append("")

    return "\n".join(lines)

def main():
    print("XTDB Feature Matrix Generator")
    print("=" * 60)
    print()

    # Run tests
    output = run_tests()

    # Parse unsupported features
    features = parse_feature_reports(output)

    # Count languages with unsupported features
    unsupported_count = sum(1 for lang, feats in features.items() if not all(feats.values()))

    print(f"\nAnalyzed {len(features)} languages")
    print(f"Found {unsupported_count} languages with unsupported features")

    # Generate matrix
    matrix = generate_matrix(features)

    if matrix:
        print("\n" + "=" * 60)
        print("FEATURE COMPATIBILITY MATRIX")
        print("=" * 60)
        print()
        print(matrix)
        print()
        print("=" * 60)
        print("Copy the above matrix into README.md")
        print("=" * 60)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()

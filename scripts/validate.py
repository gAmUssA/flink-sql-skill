#!/usr/bin/env python3
"""
Validate the Flink SQL skill structure (tessl tile format).

Usage:
    python scripts/validate.py
"""

import json
import os
import sys
import re
from pathlib import Path

TILE_DIR = Path("tiles/flink-sql")
SKILL_DIR = TILE_DIR / "skills" / "flink-sql"
SKILL_PATH = SKILL_DIR / "SKILL.md"
REF_DIR = SKILL_DIR / "references"
TILE_JSON = TILE_DIR / "tile.json"


def validate_tile_json():
    """Validate tile.json exists and has required fields."""
    errors = []

    if not TILE_JSON.exists():
        errors.append("tile.json not found!")
        return errors

    try:
        data = json.loads(TILE_JSON.read_text())
    except json.JSONDecodeError as e:
        errors.append(f"tile.json is invalid JSON: {e}")
        return errors

    for field in ("name", "version", "summary"):
        if field not in data:
            errors.append(f"tile.json missing required field '{field}'")

    if "skills" not in data and "docs" not in data and "steering" not in data:
        errors.append("tile.json must have at least one of: skills, docs, steering")

    if "skills" in data:
        for skill_name, skill_def in data["skills"].items():
            skill_path = TILE_DIR / skill_def.get("path", "")
            if not skill_path.exists():
                errors.append(f"tile.json references missing skill file: {skill_def.get('path')}")
            else:
                print(f"✅ Skill '{skill_name}' path valid: {skill_def['path']}")

    print(f"📦 Tile: {data.get('name')}@{data.get('version')}")
    return errors


def validate_skill():
    """Validate the skill structure and content."""
    errors = []
    warnings = []

    # Check SKILL.md exists
    if not SKILL_PATH.exists():
        errors.append(f"SKILL.md not found at {SKILL_PATH}!")
        return errors, warnings

    content = SKILL_PATH.read_text()
    lines = content.split('\n')

    # Check frontmatter
    if not lines[0] == '---':
        errors.append("SKILL.md missing YAML frontmatter (should start with ---)")
    else:
        # Find closing frontmatter
        try:
            end_idx = lines[1:].index('---') + 1
            frontmatter = '\n'.join(lines[1:end_idx])

            if 'name:' not in frontmatter:
                errors.append("SKILL.md missing 'name:' field in frontmatter")
            if 'description:' not in frontmatter:
                errors.append("SKILL.md missing 'description:' field in frontmatter")
        except ValueError:
            errors.append("SKILL.md has unclosed frontmatter (missing closing ---)")

    # Check size
    size = len(content)
    line_count = len(lines)

    if size > 51200:  # 50KB
        warnings.append(f"SKILL.md is large ({size} bytes). Consider moving content to references.")
    if size < 500:
        errors.append(f"SKILL.md seems too small ({size} bytes)")

    print(f"📊 SKILL.md: {size:,} bytes, {line_count} lines")

    # Check references directory
    if REF_DIR.exists():
        ref_files = list(REF_DIR.glob("*.md"))
        print(f"📁 References: {len(ref_files)} files")

        total_ref_lines = 0
        for ref_file in ref_files:
            ref_lines = len(ref_file.read_text().split('\n'))
            total_ref_lines += ref_lines
            print(f"   - {ref_file.name}: {ref_lines} lines")

        print(f"📊 Total reference lines: {total_ref_lines}")

    # Check internal links (relative to SKILL.md location)
    link_pattern = r'\[.*?\]\((references/[^)]+)\)'
    links = re.findall(link_pattern, content)

    for link in links:
        link_path = SKILL_DIR / link
        if not link_path.exists():
            errors.append(f"Broken link: {link}")
        else:
            print(f"✅ Valid link: {link}")

    # Check code blocks are closed
    code_blocks = content.count('```')
    if code_blocks % 2 != 0:
        warnings.append(f"SKILL.md may have unclosed code blocks ({code_blocks} backtick sequences)")

    return errors, warnings


def main():
    print("🔍 Validating Flink SQL Skill (Tile Format)")
    print("=" * 45)

    all_errors = []
    all_warnings = []

    # Validate tile.json
    tile_errors = validate_tile_json()
    all_errors.extend(tile_errors)

    print()

    # Validate skill content
    skill_errors, skill_warnings = validate_skill()
    all_errors.extend(skill_errors)
    all_warnings.extend(skill_warnings)

    print()

    if all_warnings:
        print("⚠️  Warnings:")
        for w in all_warnings:
            print(f"   - {w}")
        print()

    if all_errors:
        print("❌ Errors:")
        for e in all_errors:
            print(f"   - {e}")
        print()
        print("Skill validation FAILED!")
        sys.exit(1)
    else:
        print("✅ Skill validation PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    main()

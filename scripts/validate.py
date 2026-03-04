#!/usr/bin/env python3
"""
Validate the Flink SQL skill structure.

Usage:
    python scripts/validate.py
"""

import os
import sys
import re
from pathlib import Path


def validate_skill():
    """Validate the skill structure and content."""
    errors = []
    warnings = []
    
    # Check SKILL.md exists
    skill_path = Path("SKILL.md")
    if not skill_path.exists():
        errors.append("SKILL.md not found!")
        return errors, warnings
    
    content = skill_path.read_text()
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
    ref_dir = Path("references")
    if ref_dir.exists():
        ref_files = list(ref_dir.glob("*.md"))
        print(f"📁 References: {len(ref_files)} files")
        
        total_ref_lines = 0
        for ref_file in ref_files:
            ref_lines = len(ref_file.read_text().split('\n'))
            total_ref_lines += ref_lines
            print(f"   - {ref_file.name}: {ref_lines} lines")
        
        print(f"📊 Total reference lines: {total_ref_lines}")
    
    # Check internal links
    link_pattern = r'\[.*?\]\((references/[^)]+)\)'
    links = re.findall(link_pattern, content)
    
    for link in links:
        if not Path(link).exists():
            errors.append(f"Broken link: {link}")
        else:
            print(f"✅ Valid link: {link}")
    
    # Check code blocks are closed
    code_blocks = content.count('```')
    if code_blocks % 2 != 0:
        warnings.append(f"SKILL.md may have unclosed code blocks ({code_blocks} backtick sequences)")
    
    return errors, warnings


def main():
    print("🔍 Validating Flink SQL Skill")
    print("=" * 40)
    
    errors, warnings = validate_skill()
    
    print()
    
    if warnings:
        print("⚠️  Warnings:")
        for w in warnings:
            print(f"   - {w}")
        print()
    
    if errors:
        print("❌ Errors:")
        for e in errors:
            print(f"   - {e}")
        print()
        print("Skill validation FAILED!")
        sys.exit(1)
    else:
        print("✅ Skill validation PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Parse failed tests from JUnit XML files.

Supports pytest and Cypress test result formats. Extracts the file paths /
module paths of failed tests, used to retry only failed tests in CI/CD workflows.

Exit Codes:
    0: Failures found (success - list written to output file)
    1: Error during processing (fall back to full test run)
    2: No failures found (all passed - skip batch)
    3: No test results (missing artifacts - run all tests)
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Set


def parse_pytest_failures(input_dir: Path) -> Optional[Set[str]]:
    """
    Extract failed test module paths from pytest JUnit XML files.

    Returns:
        Set of relative test file paths (e.g., 'tests/structured_properties/test_structured_properties.py')
        None if no XML files found
        Empty set if all tests passed
    """
    failed: Set[str] = set()
    xml_files = list(input_dir.rglob("junit*.xml"))

    if not xml_files:
        return None

    for xml_file in xml_files:
        try:
            root = ET.parse(xml_file).getroot()
            for testcase in root.findall('.//testcase'):
                if testcase.find('failure') is not None or testcase.find('error') is not None:
                    classname = testcase.get('classname', '')
                    if classname:
                        # Convert dotted classname to file path:
                        # tests.structured_properties.test_structured_properties
                        # -> tests/structured_properties/test_structured_properties.py
                        failed.add(classname.replace('.', '/') + '.py')
        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)

    return failed


def parse_cypress_failures(input_dir: Path) -> Optional[Set[str]]:
    """
    Extract failed test file paths from Cypress JUnit XML files.

    Returns:
        Set of relative test file paths (e.g., 'mutations/dataset_ownership.js')
        None if no XML files found
        Empty set if all tests passed
    """
    failed: Set[str] = set()
    xml_files = list(input_dir.rglob("cypress-test-*.xml"))

    if not xml_files:
        return None

    for xml_file in xml_files:
        try:
            root = ET.parse(xml_file).getroot()

            # Find testsuite with file attribute to get the test file path
            file_testsuite = root.find('.//testsuite[@file]')
            if file_testsuite is None:
                continue

            file_path = file_testsuite.get('file')
            if not file_path:
                continue

            # Check if ANY testcase in the XML has failures or errors
            # (testcases are in sibling testsuites, not children of the file testsuite)
            has_failures = any(
                testcase.find('failure') is not None or testcase.find('error') is not None
                for testcase in root.findall('.//testcase')
            )

            if has_failures:
                # Strip 'cypress/e2e/' prefix to match expected format
                relative_path = file_path.removeprefix('cypress/e2e/')
                failed.add(relative_path)

        except ET.ParseError as e:
            print(f"Warning: Failed to parse {xml_file}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Error processing {xml_file}: {e}", file=sys.stderr)

    return failed


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Parse failed tests from JUnit XML files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0 - Failures found (list written to output file)
  1 - Error during processing
  2 - No failures found (all tests passed)
  3 - No test results found (missing artifacts)
        """
    )
    parser.add_argument(
        '--test-type',
        required=True,
        choices=['pytests', 'cypress'],
        help='Test framework whose results to parse'
    )
    parser.add_argument(
        '--input-dir',
        required=True,
        type=Path,
        help='Directory containing JUnit XML files'
    )
    parser.add_argument(
        '--output',
        required=True,
        type=Path,
        help='Output file path for failed test list'
    )
    args = parser.parse_args()

    try:
        parse_fn = parse_pytest_failures if args.test_type == 'pytests' else parse_cypress_failures
        failed = parse_fn(args.input_dir)

        if failed is None:
            print("No test results found", file=sys.stderr)
            sys.exit(3)

        if not failed:
            print("All tests passed - no failures to retry")
            args.output.write_text("")
            sys.exit(2)

        args.output.write_text('\n'.join(sorted(failed)) + '\n')
        print(f"Found {len(failed)} failed item(s):")
        for item in sorted(failed):
            print(f"  - {item}")
        sys.exit(0)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

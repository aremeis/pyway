import pytest
import os
from pyway.helpers import Utils
from pyway.migration import Migration


@pytest.mark.helpers_test
def test_get_local_files() -> None:
    files = Utils.get_local_files(os.path.join('tests', 'data', 'schema'))
    assert len(files) == 4  # Updated to include Python migration file


@pytest.mark.helpers_test
def test_get_local_files_notfound() -> None:
    with pytest.raises(Exception):
        _ = Utils.get_local_files(os.path.join('tests', 'datanotfound'))
    assert True


@pytest.mark.helpers_test
def test_subtract_result() -> None:
    a = [Migration.from_name('V01_01__test1.sql', os.path.join('tests', 'data', 'schema')),
         Migration.from_name('V01_02__test2.sql', os.path.join('tests', 'data', 'schema'))]
    b = [Migration.from_name('V01_01__test1.sql', os.path.join('tests', 'data', 'schema'))]
    c = [Migration.from_name('V01_02__test2.sql', os.path.join('tests', 'data', 'schema'))]
    d = Utils.subtract(a, b)

    assert c[0].name == d[0].name


@pytest.mark.helpers_test
def test_subtract_noresult() -> None:
    a = [Migration.from_name('V01_01__test1.sql', os.path.join('tests', 'data', 'schema'))]
    b = [Migration.from_name('V01_01__test1.sql', os.path.join('tests', 'data', 'schema'))]
    c = Utils.subtract(a, b)

    assert c == []


@pytest.mark.helpers_test
def test_subtract_onlyonearray() -> None:
    a = [Migration.from_name('V01_01__test1.sql', os.path.join('tests', 'data', 'schema'))]
    b = []
    c = Utils.subtract(a, b)

    assert c == a


@pytest.mark.helpers_test
def test_expected_pattern() -> None:
    pattern = Utils.expected_pattern()
    assert pattern == "V{major}_{minor}_{patch}__{description}[.sql|.py]"


@pytest.mark.helpers_test
def test_get_version_from_name() -> None:
    with pytest.raises(Exception):
        _ = Utils.get_version_from_name("test1.sql")
    assert True


@pytest.mark.helpers_test
def test_load_checksum_from_name_failed() -> None:
    with pytest.raises(Exception):
        _ = Utils.load_checksum_from_name('test', 'test')
    assert True


@pytest.mark.helpers_test
def test_version_name() -> None:
    assert Utils.is_file_name_valid('V1_1__test1.sql')


@pytest.mark.helpers_test
def test_semantic_version_name() -> None:
    assert Utils.is_file_name_valid('V1_0_1__test1.sql')


@pytest.mark.helpers_test
def test_semantic_version_name_major_period() -> None:
    assert Utils.is_file_name_valid('V1.0_1__test1.sql')


@pytest.mark.helpers_test
def test_semantic_version_name_minor_period() -> None:
    assert Utils.is_file_name_valid('V1_0.1__test1.sql')


@pytest.mark.helpers_test
def test_semantic_version_name_major_minor_period() -> None:
    assert Utils.is_file_name_valid('V1.0.1__test1.sql')


@pytest.mark.helpers_test
def test_semantic_version_name_minor_over_2digits() -> None:
    assert not Utils.is_file_name_valid('V1_0_100__test1.sql')


@pytest.mark.helpers_test
def test_version_name_major_only() -> None:
    """Test that names with only major version are valid"""
    assert Utils.is_file_name_valid('V1__test1.sql')
    assert Utils.is_file_name_valid('V1__test1.py')
    assert Utils.is_file_name_valid('V99__test1.sql')  # 2 digits max


# Test illegal version names
@pytest.mark.helpers_test
def test_invalid_version_name_missing_prefix() -> None:
    """Test that names without the V prefix are rejected"""
    assert not Utils.is_file_name_valid('1_1__test1.sql')
    assert not Utils.is_file_name_valid('1.1__test1.sql')
    assert not Utils.is_file_name_valid('1_0_1__test1.sql')


@pytest.mark.helpers_test
def test_invalid_version_name_missing_separator() -> None:
    """Test that names without the __ separator are rejected"""
    assert not Utils.is_file_name_valid('V1_1_test1.sql')
    assert not Utils.is_file_name_valid('V1.1_test1.sql')
    assert not Utils.is_file_name_valid('V1_0_1_test1.sql')


@pytest.mark.helpers_test
def test_invalid_version_name_missing_extension() -> None:
    """Test that names without proper extensions are rejected"""
    assert not Utils.is_file_name_valid('V1_1__test1')
    assert not Utils.is_file_name_valid('V1_1__test1.txt')
    assert not Utils.is_file_name_valid('V1_1__test1.js')


@pytest.mark.helpers_test
def test_invalid_version_name_malformed_version() -> None:
    """Test that malformed version numbers are rejected"""
    assert Utils.is_file_name_valid('V1__test1.sql')
    assert not Utils.is_file_name_valid('V__test1.sql')  # Missing major version
    assert not Utils.is_file_name_valid('V1._test1.sql')  # Incomplete minor version


@pytest.mark.helpers_test
def test_invalid_version_name_wrong_format() -> None:
    """Test various wrong format combinations"""
    assert Utils.is_file_name_valid('V1.1.1__test1.sql')
    assert Utils.is_file_name_valid('V1_1_1__test1.sql')
    assert Utils.is_file_name_valid('V1_1__test1__extra.sql')
    assert not Utils.is_file_name_valid('V1.1.1.1__test1.sql')  # 4 version components


@pytest.mark.helpers_test
def test_invalid_version_name_empty_parts() -> None:
    """Test names with empty parts"""
    assert not Utils.is_file_name_valid('V__test1.sql')  # Empty version
    assert not Utils.is_file_name_valid('V1_1__.sql')  # Empty description


@pytest.mark.helpers_test
def test_invalid_version_name_special_characters() -> None:
    """Test names with special characters"""
    assert not Utils.is_file_name_valid('V1@1__test1.sql')  # Invalid character in version
    assert not Utils.is_file_name_valid('V1#1__test1.sql')  # Invalid character in version
    assert not Utils.is_file_name_valid('V1$1__test1.sql')  # Invalid character in version


@pytest.mark.helpers_test
def test_invalid_version_name_whitespace() -> None:
    """Test names with whitespace"""
    assert not Utils.is_file_name_valid('V1 1__test1.sql')  # Space in version
    assert not Utils.is_file_name_valid('V1_1__ test1.sql')  # Space after separator
    assert not Utils.is_file_name_valid(' V1_1__test1.sql')  # Leading space
    assert not Utils.is_file_name_valid('V1_1__test1.sql ')  # Trailing space


@pytest.mark.helpers_test
def test_invalid_version_name_case_sensitivity() -> None:
    """Test case sensitivity (should be case insensitive for extensions)"""
    assert Utils.is_file_name_valid('V1_1__test1.SQL')  # Uppercase extension
    assert Utils.is_file_name_valid('V1_1__test1.PY')  # Uppercase Python extension


@pytest.mark.helpers_test
def test_invalid_version_name_edge_cases() -> None:
    """Test edge cases and boundary conditions"""
    assert not Utils.is_file_name_valid('')  # Empty string
    assert not Utils.is_file_name_valid('V')  # Just prefix
    assert not Utils.is_file_name_valid('V__')  # Prefix and separator only
    assert not Utils.is_file_name_valid('V1__')  # Prefix, version, separator only
    assert not Utils.is_file_name_valid('__test1.sql')  # Missing prefix and version

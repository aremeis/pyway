import os
import re
import zlib
from typing import Any, Dict, List, Iterable, Tuple

from pyway import settings
from pyway.errors import VALID_NAME_ERROR, DIRECTORY_NOT_FOUND, OUT_OF_DATE_ERROR


class bcolors():
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Utils():

    @staticmethod
    def _version_sort_key(version: str) -> Tuple[int, ...]:
        """Convert version string to tuple of ints for correct numeric sorting."""
        return tuple(int(c) for c in version.replace("_", ".").split("."))

    @staticmethod
    def subtract(list_a: List, list_b: List) -> List:
        result = []
        if list_a and list_b:
            version_set_b = {Utils._version_sort_key(b.version) for b in list_b}
            result = [a for a in list_a if Utils._version_sort_key(a.version) not in version_set_b]
        elif list_a and not list_b:
            # List B is empty (usually from a new install)
            return list_a
        return result

    @staticmethod
    def expected_pattern() -> str:
        return f'{settings.SQL_MIGRATION_PREFIX}{{version}}{settings.SQL_MIGRATION_SEPARATOR}' \
                f'{{description}}[{settings.SQL_MIGRATION_SUFFIXES}|.py]'

    @staticmethod
    def is_file_name_valid(name: str) -> bool:
        template = r"^%s\d+(?:[._]\d+)*%s([A-Za-z0-9_]+(?:%s[A-Za-z0-9_]+)*)(\%s|\.py)$"
        _pattern = template % (
            re.escape(settings.SQL_MIGRATION_PREFIX),
            re.escape(settings.SQL_MIGRATION_SEPARATOR),
            re.escape(settings.SQL_MIGRATION_SEPARATOR),
            settings.SQL_MIGRATION_SUFFIXES
        )
        return re.fullmatch(_pattern, name, re.IGNORECASE) is not None

    @staticmethod
    def sort_migrations_list(migrations: List[Any]) -> List[Any]:
        def sort_key(x: Any) -> Tuple[Tuple[int, ...], str]:
            if isinstance(x, dict):
                version = x.get("version", "")
                name = x.get("name", "")
            else:
                version = x.version
                name = x.name
            return (Utils._version_sort_key(version), name)
        return sorted(migrations, key=sort_key)

    @staticmethod
    def flatten_migrations(migrations: Iterable[Any]) -> List[Dict[Any, Any]]:
        migration_list = []
        for migration in migrations:
            migration_list.append({'version': Utils.format_version(migration.version), 'extension': migration.extension,
                                   'name': migration.name, 'checksum': migration.checksum,
                                   'apply_timestamp': migration.apply_timestamp})
        return migration_list

    @staticmethod
    def get_version_from_name(name: str) -> str:
        pattern = rf"^{re.escape(settings.SQL_MIGRATION_PREFIX)}([\d._]+){re.escape(settings.SQL_MIGRATION_SEPARATOR)}"
        match = re.match(pattern, name)

        if not match:
            raise ValueError(VALID_NAME_ERROR % (name, Utils.expected_pattern()))

        version_part = match.group(1)
        # Normalize separators: replace _ with .
        version = version_part.replace("_", ".")

        return version

    @staticmethod
    def get_extension_from_name(name: str) -> str:
        return name.split('.')[-1].upper()

    @staticmethod
    def load_checksum_from_name(name: str, path: str) -> str:
        fullname = os.path.join(os.getcwd(), path, name)
        prev = 0
        try:
            for line in open(fullname, "rb"):
                prev = zlib.crc32(line, prev)
            return "%X" % (prev & 0xFFFFFFFF)
        except FileNotFoundError:
            raise FileNotFoundError(OUT_OF_DATE_ERROR % fullname.split("/")[-1])

    @staticmethod
    def basepath(d: str) -> str:
        return os.path.join(os.getcwd(), d)

    @staticmethod
    def get_local_files(d: str) -> List[str]:
        path = Utils.basepath(d)
        dir_list = []
        try:
            # Skip any hidden files and directories
            for f in os.listdir(path):
                full_path = os.path.join(path, f)
                if not f.startswith('.') and os.path.isfile(full_path):
                    dir_list.append(f)
        except OSError:
            raise FileNotFoundError(DIRECTORY_NOT_FOUND % path)
        return dir_list

    @staticmethod
    def create_map_from_list(key: str, list_: List[Any]) -> Dict[Any, Any]:
        return {lst.__dict__[key]: lst for lst in list_}

    @staticmethod
    def color(msg: str, color: str) -> str:
        return f"{color}{msg}{bcolors.ENDC}"

    @staticmethod
    def check_required_vars(required_keys: List[str], obj: Any) -> bool:
        missing_keys = []
        for key in required_keys:
            if not getattr(obj, key):
                missing_keys.append(key)

        if missing_keys:
            raise KeyError(f"Missing configuration options: {', '.join(missing_keys)}")
        return True

    @staticmethod
    def format_version(version: str) -> str:
        """Normalize version string (replace _ with .)."""
        return version.replace("_", ".")

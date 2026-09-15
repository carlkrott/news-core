"""Phase 4 Slice 5: deterministic, path-free wrapper reliability analysis."""
from __future__ import annotations

import ast
from dataclasses import dataclass
from enum import Enum
import re


class SourceKind(str, Enum):
    SHELL = "SHELL"
    PYTHON = "PYTHON"


class Severity(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    CRITICAL = "CRITICAL"


class FindingCode(str, Enum):
    FINAL_FALSE_CONDITIONAL_EXIT = "FINAL_FALSE_CONDITIONAL_EXIT"
    DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL = "DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL"
    OUR_SETUP_OMITTED = "OUR_SETUP_OMITTED"
    TRUNCATION_LIMIT_MISMATCH = "TRUNCATION_LIMIT_MISMATCH"
    CURL_RESULT_SUPPRESSED = "CURL_RESULT_SUPPRESSED"
    EXCEPTION_TEXT_CAN_REACH_PAYLOAD = "EXCEPTION_TEXT_CAN_REACH_PAYLOAD"
    HARDCODED_CREDENTIAL_ASSIGNMENT = "HARDCODED_CREDENTIAL_ASSIGNMENT"
    WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL = "WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL"


_SEVERITY = {
    FindingCode.FINAL_FALSE_CONDITIONAL_EXIT: Severity.HIGH,
    FindingCode.DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL: Severity.HIGH,
    FindingCode.OUR_SETUP_OMITTED: Severity.HIGH,
    FindingCode.TRUNCATION_LIMIT_MISMATCH: Severity.MEDIUM,
    FindingCode.CURL_RESULT_SUPPRESSED: Severity.HIGH,
    FindingCode.EXCEPTION_TEXT_CAN_REACH_PAYLOAD: Severity.CRITICAL,
    FindingCode.HARDCODED_CREDENTIAL_ASSIGNMENT: Severity.CRITICAL,
    FindingCode.WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL: Severity.HIGH,
}


@dataclass(frozen=True, slots=True)
class SourceText:
    virtual_path: str
    kind: SourceKind
    text: str

    def __post_init__(self) -> None:
        if type(self.virtual_path) is not str or not self.virtual_path:
            raise TypeError("SourceText.virtual_path must be a non-empty str")
        if not isinstance(self.kind, SourceKind):
            raise TypeError("SourceText.kind must be SourceKind")
        if type(self.text) is not str:
            raise TypeError("SourceText.text must be str")


@dataclass(frozen=True, slots=True)
class HealthRecord:
    run_id: str
    wrapper_path: str
    exit_code: int
    stderr_code: str | None
    model_error_code: str | None
    summary_payload_sent: bool

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or not self.run_id:
            raise TypeError("HealthRecord.run_id must be a non-empty str")
        if type(self.wrapper_path) is not str or not self.wrapper_path:
            raise TypeError("HealthRecord.wrapper_path must be a non-empty str")
        if type(self.exit_code) is not int or isinstance(self.exit_code, bool):
            raise TypeError("HealthRecord.exit_code must be int")
        if self.stderr_code is not None and type(self.stderr_code) is not str:
            raise TypeError("HealthRecord.stderr_code must be str or None")
        if self.model_error_code is not None and type(self.model_error_code) is not str:
            raise TypeError("HealthRecord.model_error_code must be str or None")
        if type(self.summary_payload_sent) is not bool:
            raise TypeError("HealthRecord.summary_payload_sent must be bool")


@dataclass(frozen=True, slots=True)
class Evidence:
    """The only retained source evidence: path, line, code, and safe excerpt."""

    virtual_path: str
    line_number: int
    finding_code: FindingCode
    excerpt: str

    def __post_init__(self) -> None:
        if type(self.virtual_path) is not str or not self.virtual_path:
            raise TypeError("Evidence.virtual_path must be a non-empty str")
        if type(self.line_number) is not int or isinstance(self.line_number, bool):
            raise TypeError("Evidence.line_number must be int")
        if self.line_number < 1:
            raise ValueError("Evidence.line_number must be 1-based")
        if not isinstance(self.finding_code, FindingCode):
            raise TypeError("Evidence.finding_code must be FindingCode")
        if type(self.excerpt) is not str:
            raise TypeError("Evidence.excerpt must be str")
        if len(self.excerpt) > 120:
            raise ValueError("Evidence.excerpt must be at most 120 characters")

    @property
    def severity(self) -> Severity:
        return _SEVERITY[self.finding_code]


Finding = Evidence
FindingSeverity = Severity


@dataclass(frozen=True, slots=True)
class ReliabilityResult:
    findings: tuple[Evidence, ...]

    def __post_init__(self) -> None:
        if type(self.findings) is not tuple:
            raise TypeError("ReliabilityResult.findings must be tuple")
        if any(not isinstance(item, Evidence) for item in self.findings):
            raise TypeError("ReliabilityResult.findings must contain Evidence")


ReliabilityAnalysis = ReliabilityResult


_ASSIGNMENT_RE = re.compile(
    r"^\s*(?:(?:export|local|declare)\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$"
)
_PROCESS_RE = re.compile(r"(?:^|&&\s*)process_category\s+([^\s;&|]+)")
_CURL_RE = re.compile(r"^\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*=\s*)?curl(?:\s|$)")
_SENSITIVE_RE = re.compile(r"token|secret|password|authorization|api[_-]?key", re.IGNORECASE)
_HEREDOC_RE = re.compile(r"python3[^\n]*<<\s*['\"]?([A-Za-z_][A-Za-z0-9_-]*)['\"]?")


def _shell_code(line: str) -> str:
    quote: str | None = None
    escaped = False
    out: list[str] = []
    for char in line:
        if escaped:
            out.append(char)
            escaped = False
            continue
        if char == "\\" and quote != "'":
            out.append(char)
            escaped = True
            continue
        if quote is not None:
            out.append(char)
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            out.append(char)
        elif char == "#":
            break
        else:
            out.append(char)
    return "".join(out).rstrip()


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _shell_lines(source: SourceText) -> list[tuple[int, str]]:
    return [(number, _shell_code(line)) for number, line in enumerate(source.text.splitlines(), 1) if _shell_code(line).strip()]


def _assignment(line: str) -> tuple[str, str] | None:
    match = _ASSIGNMENT_RE.match(line)
    if not match:
        return None
    return match.group(1), match.group(2)


def _command_segments(line: str) -> list[str]:
    segments: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(line):
        char = line[index]
        if escaped:
            current.append(char)
            escaped = False
            index += 1
            continue
        if char == "\\" and quote != "'":
            current.append(char)
            escaped = True
            index += 1
            continue
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
            current.append(char)
            index += 1
            continue
        if char == ";" or (char == "&" and index + 1 < len(line) and line[index + 1] == "&"):
            segments.append("".join(current))
            current = []
            index += 2 if char == "&" else 1
            continue
        current.append(char)
        index += 1
    segments.append("".join(current))
    return segments


def _process_category(line: str) -> str | None:
    for segment in _command_segments(line):
        command = segment.strip()
        if not command.startswith("process_category"):
            continue
        remainder = command[len("process_category"):]
        if not remainder or not remainder[0].isspace():
            continue
        return _unquote(remainder.strip().split()[0])
    return None


def _is_unconditional_success(line: str) -> bool:
    value = line.strip()
    return value in {":", "true", "exit 0", "return 0"} or value.startswith("exit 0 ")


def _literal_credential(rhs: str) -> bool:
    value = rhs.strip()
    if not value or "$" in value or "`" in value or value.startswith(("<", "read ", "cat ")):
        return False
    if value.startswith(("\"", "'")):
        return len(value) >= 2 and value[-1] == value[0] and value[1:-1] != ""
    return not value.startswith(("{", "["))


def _evidence(path: str, line: int, code: FindingCode, excerpt: str) -> Evidence:
    return Evidence(path, max(1, line), code, excerpt[:120])


def _shell_findings(source: SourceText) -> list[Evidence]:
    lines = _shell_lines(source)
    if not lines:
        return []
    findings: list[Evidence] = []
    assignments: dict[str, list[tuple[int, str]]] = {"fantasy": [], "avind": [], "oursetup": []}
    calls: dict[str, list[int]] = {"fantasy": [], "avind": [], "oursetup": []}
    conditional_lines: list[int] = []

    for number, line in lines:
        assignment = _assignment(line)
        if assignment is not None:
            name, rhs = assignment
            folded = name.casefold()
            if "fantasy" in folded:
                assignments["fantasy"].append((number, rhs))
            if "avind" in folded:
                assignments["avind"].append((number, rhs))
            if "oursetup" in folded or "our_setup" in folded:
                assignments["oursetup"].append((number, rhs))
            if _SENSITIVE_RE.search(name) and _literal_credential(rhs) and "${" not in rhs:
                findings.append(_evidence(source.virtual_path, number, FindingCode.HARDCODED_CREDENTIAL_ASSIGNMENT, f"{name}=<redacted>"))

        category = _process_category(line)
        if category is not None:
            folded_category = category.casefold().replace("_", "-")
            if folded_category in {"fantasy-novel", "fantasy"}:
                calls["fantasy"].append(number)
            elif folded_category in {"audiovisual", "avind", "av-industry"}:
                calls["avind"].append(number)
            elif folded_category in {"our-setup", "oursetup"}:
                calls["oursetup"].append(number)

        if re.match(r"^\s*\[\[.*\]\]\s*&&\s*process_category\b", line):
            conditional_lines.append(number)

    if any(len(assignments[key]) > 1 or len(calls[key]) > 1 for key in ("fantasy", "avind")):
        duplicate_line = next(
            (items[1][0] for key in ("fantasy", "avind") for items in (assignments[key],) if len(items) > 1),
            None,
        )
        if duplicate_line is None:
            duplicate_line = next(
                (calls[key][1] for key in ("fantasy", "avind") if len(calls[key]) > 1),
                1,
            )
        counts = ", ".join(f"{key.upper()}={len(assignments[key]) + len(calls[key])}" for key in ("fantasy", "avind") if len(assignments[key]) > 1 or len(calls[key]) > 1)
        findings.append(_evidence(source.virtual_path, duplicate_line, FindingCode.DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL, f"duplicate category assignment/call ({counts})"))

    if not assignments["oursetup"] or not calls["oursetup"]:
        line = assignments["oursetup"][0][0] if assignments["oursetup"] else (calls["oursetup"][0] if calls["oursetup"] else 1)
        findings.append(_evidence(source.virtual_path, line, FindingCode.OUR_SETUP_OMITTED, "OURSETUP assignment and process_category call are required"))

    if conditional_lines:
        last_conditional = conditional_lines[-1]
        later = [line for number, line in lines if number > last_conditional]
        if not any(_is_unconditional_success(line) for line in later):
            findings.append(_evidence(source.virtual_path, last_conditional, FindingCode.FINAL_FALSE_CONDITIONAL_EXIT, "final conditional process_category lacks unconditional success"))

    for number, line in lines:
        if not _CURL_RE.match(line):
            continue
        quiet_without_error = bool(re.search(r"(?:^|\s)(?:-s|--silent)(?:\s|$)", line)) and not bool(re.search(r"(?:^|\s)(?:-sS|-Ss|-S|--show-error)(?:\s|$)", line))
        suppressed = bool(re.search(r"(?:>|\s-o\s+|\s--output\s+)/dev/null(?:\s|$)", line))
        checked = bool(re.search(r"(?:^|\s)(?:--fail|--fail-with-body|-f)(?:\s|$)", line)) or "--write-out" in line or " -w " in f" {line} "
        if suppressed or quiet_without_error or not checked:
            findings.append(_evidence(source.virtual_path, number, FindingCode.CURL_RESULT_SUPPRESSED, "curl result is suppressed or lacks failure/HTTP-status handling"))
            break
    return findings


def _constant_slice(node: ast.Subscript) -> int | None:
    value = node.slice
    if isinstance(value, ast.Constant) and type(value.value) is int:
        return value.value
    if isinstance(value, ast.Slice) and isinstance(value.upper, ast.Constant) and type(value.upper.value) is int:
        return value.upper.value
    return None


def _slice_caps(tree: ast.AST) -> tuple[bool, bool]:
    embedded = False
    standalone = False
    input_names = {
        "raw",
        "raw_content",
        "raw_text",
        "content",
        "text",
        "body",
        "input_data",
        "source_text",
        "news_content",
    }
    for function in (node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
        function_name = function.name.casefold()
        function_context = "summar" in function_name or "brief" in function_name
        for node in ast.walk(function):
            if not isinstance(node, ast.Subscript):
                continue
            cap = _constant_slice(node)
            base_name = getattr(node.value, "id", "").casefold()
            if cap not in (5000, 8000) or not (base_name in input_names or function_context):
                continue
            if cap == 5000:
                embedded = True
            else:
                standalone = True
    # Also support small module-level summarizer snippets used by callers.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript):
            continue
        cap = _constant_slice(node)
        base_name = getattr(node.value, "id", "").casefold()
        if cap == 5000 and base_name in input_names:
            embedded = True
        elif cap == 8000 and base_name in input_names:
            standalone = True
    return embedded, standalone


def _name_used(node: ast.AST, name: str) -> bool:
    return any(isinstance(item, ast.Name) and item.id == name and isinstance(item.ctx, ast.Load) for item in ast.walk(node))


def _is_stderr_print(node: ast.Call) -> bool:
    if not isinstance(node.func, ast.Name) or node.func.id != "print":
        return False
    for keyword in node.keywords:
        if keyword.arg == "file":
            if isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "stderr":
                return True
            if isinstance(keyword.value, ast.Name) and keyword.value.id.casefold() in {"stderr", "err"}:
                return True
    return False


def _exception_text_functions(tree: ast.AST) -> tuple[set[str], int | None]:
    vulnerable: set[str] = set()
    first_line: int | None = None
    functions = [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    for function in functions:
        for handler in [node for node in ast.walk(function) if isinstance(node, ast.ExceptHandler)]:
            if not handler.name:
                continue
            exception_name = handler.name
            formatted = False
            output = False
            assigned_text: set[str] = set()
            for item in ast.walk(handler):
                if isinstance(item, ast.Name) and item.id == exception_name and isinstance(item.ctx, ast.Load):
                    formatted = True
                if isinstance(item, ast.Assign) and _name_used(item.value, exception_name):
                    for target in item.targets:
                        if isinstance(target, ast.Name):
                            assigned_text.add(target.id)
                if isinstance(item, ast.Call) and _name_used(item, exception_name) and not _is_stderr_print(item):
                    if isinstance(item.func, ast.Name) and item.func.id in {"print", "send_telegram"}:
                        output = True
            if not formatted:
                continue
            for item in ast.walk(handler):
                if isinstance(item, ast.Return) and item.value is not None:
                    if _name_used(item.value, exception_name) or any(isinstance(name, ast.Name) and name.id in assigned_text for name in ast.walk(item.value)):
                        output = True
            if output:
                vulnerable.add(function.name)
                first_line = first_line or getattr(handler, "lineno", getattr(function, "lineno", 1))
    return vulnerable, first_line


def _python_payload_path(tree: ast.AST) -> tuple[bool, int | None]:
    vulnerable, first_line = _exception_text_functions(tree)
    if not vulnerable:
        return False, None
    exception_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and node.name
    }
    calls_from_vulnerable: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id in vulnerable:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    calls_from_vulnerable.add(target.id)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "send_telegram":
            if any(_name_used(arg, name) for name in exception_names for arg in node.args):
                return True, getattr(node, "lineno", first_line or 1)
            if any((isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name) and arg.func.id in vulnerable) or (isinstance(arg, ast.Name) and arg.id in calls_from_vulnerable) for arg in node.args):
                return True, getattr(node, "lineno", first_line or 1)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
            if any(_name_used(arg, name) for name in vulnerable for arg in node.args) and not _is_stderr_print(node):
                return True, getattr(node, "lineno", first_line or 1)
    return False, None


def _extract_heredocs(source: SourceText) -> list[tuple[int, str, str | None]]:
    raw_lines = source.text.splitlines()
    blocks: list[tuple[int, str, str | None]] = []
    index = 0
    while index < len(raw_lines):
        match = _HEREDOC_RE.search(raw_lines[index])
        if not match:
            index += 1
            continue
        tag = match.group(1)
        end = index + 1
        while end < len(raw_lines) and raw_lines[end].strip() != tag:
            end += 1
        body = "\n".join(raw_lines[index + 1:end])
        assignment_match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\$\(\s*python3", raw_lines[index])
        assignment = assignment_match.group(1) if assignment_match else None
        blocks.append((index + 2, body, assignment))
        index = end + 1
    return blocks


def _python_findings(source: SourceText) -> list[Evidence]:
    findings: list[Evidence] = []
    snippets: list[tuple[int, str, str | None]] = [(1, source.text, None)] if source.kind is SourceKind.PYTHON else _extract_heredocs(source)
    embedded = False
    standalone = False
    payload_line: int | None = None
    for start_line, text, assignment in snippets:
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        found_embedded, found_standalone = _slice_caps(tree)
        embedded = embedded or found_embedded
        standalone = standalone or found_standalone
        path_found, path_line = _python_payload_path(tree)
        if path_found:
            payload_line = start_line + (path_line or 1) - 1
        vulnerable_functions, vulnerable_line = _exception_text_functions(tree)
        if source.kind is SourceKind.SHELL and vulnerable_functions:
            raw_lines = source.text.splitlines()
            assignment_names: set[str] = set()
            for number, line in enumerate(raw_lines, 1):
                code = _shell_code(line)
                assignment_match = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\$\(", code)
                if assignment_match and number >= start_line:
                    assignment_names.add(assignment_match.group(1))
                if assignment and number >= start_line and re.search(rf"send_telegram\s+.*\${re.escape(assignment)}\b", code):
                    payload_line = number
                    break
                if assignment_names and re.search(r"\bsend_telegram\b", code):
                    if any(re.search(rf"\$(?:\{{)?{re.escape(name)}\b", code) for name in assignment_names):
                        payload_line = number
                        break
            if payload_line is None and path_found:
                payload_line = start_line + (path_line or vulnerable_line or 1) - 1
    if embedded and standalone:
        findings.append(_evidence(source.virtual_path, 1, FindingCode.TRUNCATION_LIMIT_MISMATCH, "summarization slice caps differ: embedded=5000 standalone=8000"))
    if payload_line is not None:
        findings.append(_evidence(source.virtual_path, payload_line, FindingCode.EXCEPTION_TEXT_CAN_REACH_PAYLOAD, "caught exception text reaches send_telegram payload"))
    return findings


class ReliabilityAnalyzer:
    __slots__ = ()

    def analyze(self, sources: tuple[SourceText, ...], health_records: tuple[HealthRecord, ...] = ()) -> ReliabilityResult:
        if type(sources) is not tuple or type(health_records) is not tuple:
            raise TypeError("sources and health_records must be tuples")
        if any(not isinstance(source, SourceText) for source in sources):
            raise TypeError("sources must contain SourceText")
        if any(not isinstance(record, HealthRecord) for record in health_records):
            raise TypeError("health_records must contain HealthRecord")
        findings: list[Evidence] = []
        by_path: dict[str, list[Evidence]] = {}
        for source in sources:
            source_findings = _shell_findings(source) if source.kind is SourceKind.SHELL else []
            source_findings.extend(_python_findings(source))
            by_path[source.virtual_path] = source_findings
            findings.extend(source_findings)
        for record in health_records:
            if record.exit_code == 0 or record.model_error_code is not None:
                continue
            structural = [
                item for item in by_path.get(record.wrapper_path, ())
                if item.finding_code in {
                    FindingCode.FINAL_FALSE_CONDITIONAL_EXIT,
                    FindingCode.DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL,
                    FindingCode.OUR_SETUP_OMITTED,
                    FindingCode.CURL_RESULT_SUPPRESSED,
                }
            ]
            if structural:
                findings.append(_evidence(record.wrapper_path, structural[0].line_number, FindingCode.WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL, "nonzero wrapper exit explained by structural wrapper finding"))
        findings.sort(key=lambda item: (list(FindingCode).index(item.finding_code), item.virtual_path, item.line_number))
        return ReliabilityResult(tuple(findings))

    __call__ = analyze


def analyze_reliability(sources: tuple[SourceText, ...], health_records: tuple[HealthRecord, ...] = ()) -> ReliabilityResult:
    return ReliabilityAnalyzer().analyze(sources, health_records)


__all__ = (
    "Evidence",
    "Finding",
    "FindingCode",
    "FindingSeverity",
    "HealthRecord",
    "ReliabilityAnalysis",
    "ReliabilityAnalyzer",
    "ReliabilityResult",
    "Severity",
    "SourceKind",
    "SourceText",
    "analyze_reliability",
)

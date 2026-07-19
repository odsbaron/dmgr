import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).parents[2] / "src" / "quant_research"
FORBIDDEN_PREFIXES = ("quant_research.labels", "quant_research.evaluation")


def test_factors_and_features_have_no_reverse_label_or_evaluation_dependency():
    violations = []
    for package_name in ("factors", "features"):
        for source_path in sorted((PACKAGE_ROOT / package_name).rglob("*.py")):
            for node in ast.walk(ast.parse(source_path.read_text(encoding="utf-8"))):
                imported = _imported_modules(node)
                for module in imported:
                    if module.startswith(FORBIDDEN_PREFIXES):
                        violations.append(f"{source_path.relative_to(PACKAGE_ROOT)} -> {module}")

    assert violations == []


def _imported_modules(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    if isinstance(node, ast.ImportFrom) and node.module:
        return (node.module,)
    return ()

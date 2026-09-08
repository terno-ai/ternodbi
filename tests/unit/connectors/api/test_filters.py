"""The filter grammar parser (§6.8).

Pure and central: every connector lowers this AST rather than parsing the string
itself, so these edge cases are exercised once here rather than N times badly.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.filters import parse_filters


class TestHappyPath:
    def test_empty_is_none(self):
        assert parse_filters(None) is None
        assert parse_filters("") is None
        assert parse_filters("   ") is None

    def test_single_condition(self):
        ast = parse_filters("clicks > 100")
        assert ast.conjunction == "AND"
        assert len(ast.conditions) == 1
        c = ast.conditions[0]
        assert (c.field, c.operator, c.value) == ("clicks", ">", "100")

    def test_and_chain(self):
        ast = parse_filters("country == US AND clicks > 100")
        assert ast.conjunction == "AND"
        assert [c.field for c in ast.conditions] == ["country", "clicks"]

    def test_or_chain(self):
        ast = parse_filters("spend >= 1000 OR clicks > 500")
        assert ast.conjunction == "OR"
        assert len(ast.conditions) == 2

    @pytest.mark.parametrize("op", [
        "==", "!=", ">", ">=", "<", "<=", "=@", "!@", "=~", "!~",
    ])
    def test_every_scalar_operator(self, op):
        ast = parse_filters(f"field {op} value")
        assert ast.conditions[0].operator == op

    def test_longest_operator_wins(self):
        # '>=' must not be read as '>' then '=value'.
        assert parse_filters("x >= 5").conditions[0].operator == ">="
        assert parse_filters("x <= 5").conditions[0].operator == "<="
        assert parse_filters("x != 5").conditions[0].operator == "!="

    def test_in_list_operator(self):
        ast = parse_filters("country [] US,CA,GB")
        c = ast.conditions[0]
        assert c.operator == "[]"
        assert c.value == ["US", "CA", "GB"]

    def test_quoted_value_may_contain_spaces(self):
        ast = parse_filters('campaign =@ "Black Friday"')
        assert ast.conditions[0].value == "Black Friday"

    def test_quoted_value_may_contain_and(self):
        # 'AND' inside quotes must not split the expression.
        ast = parse_filters('name == "Ben AND Jerry" AND clicks > 1')
        assert len(ast.conditions) == 2
        assert ast.conditions[0].value == "Ben AND Jerry"

    def test_dotted_field_names(self):
        ast = parse_filters("session.source == google")
        assert ast.conditions[0].field == "session.source"

    def test_conjunction_is_case_insensitive(self):
        assert parse_filters("a == 1 and b == 2").conjunction == "AND"
        assert parse_filters("a == 1 or b == 2").conjunction == "OR"


class TestErrors:
    def _code(self, expr):
        with pytest.raises(ApiError) as exc:
            parse_filters(expr)
        assert exc.value.code == ErrorCode.INVALID_FILTER
        return exc.value

    def test_mixed_and_or_is_rejected(self):
        # Rather than silently pick a precedence.
        err = self._code("a == 1 AND b == 2 OR c == 3")
        assert "split" in err.message.lower()

    def test_unterminated_quote(self):
        self._code('name == "unclosed')

    def test_missing_operator(self):
        self._code("clicks 100")

    def test_missing_value(self):
        self._code("clicks >")

    def test_empty_in_list(self):
        self._code("country [] ")

    def test_value_without_field(self):
        self._code("== value")

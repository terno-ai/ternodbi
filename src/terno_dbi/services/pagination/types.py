from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class OrderColumn:
    column: str
    direction: str = "DESC"
    nulls: str = "LAST"

    def inverted(self) -> "OrderColumn":
        """Return column with inverted direction for backward paging."""
        return OrderColumn(
            column=self.column,
            direction="ASC" if self.direction.upper() == "DESC" else "DESC",
            nulls=self.nulls
        )


class PaginationMode(Enum):
    """Pagination strategy modes."""
    OFFSET = "offset"
    CURSOR = "cursor"
    STREAM = "stream"

@dataclass
class PaginationConfig:
    mode: PaginationMode = PaginationMode.OFFSET
    page: int = 1
    per_page: int = 50
    cursor: Optional[str] = None
    direction: str = "forward"
    order_by: List[OrderColumn] = field(default_factory=list)

@dataclass
class PaginatedResult:
    data: List[Tuple]
    columns: List[str]
    page: int
    per_page: int
    total_count: Optional[int]
    total_pages: Optional[int]
    has_next: bool
    has_prev: bool
    next_cursor: Optional[str]
    prev_cursor: Optional[str]
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "data": [list(row) for row in self.data],
            "columns": self.columns,
            "page": self.page,
            "per_page": self.per_page,
            "total_count": self.total_count,
            "total_pages": self.total_pages,
            "has_next": self.has_next,
            "has_prev": self.has_prev,
            "next_cursor": self.next_cursor,
            "prev_cursor": self.prev_cursor,
        }

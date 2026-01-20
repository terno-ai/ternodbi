from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class OrderColumn:
    """Single column in ORDER BY clause."""
    column: str
    direction: str = "DESC"  # ASC or DESC
    nulls: str = "LAST"      # FIRST or LAST
    
    def inverted(self) -> "OrderColumn":
        """Return column with inverted direction for backward paging."""
        return OrderColumn(
            column=self.column,
            direction="ASC" if self.direction.upper() == "DESC" else "DESC",
            nulls=self.nulls
        )

class PaginationMode(Enum):
    """Pagination strategy modes."""
    OFFSET = "offset"   # Traditional LIMIT/OFFSET
    CURSOR = "cursor"   # Keyset pagination
    STREAM = "stream"   # Server-side streaming

@dataclass
class PaginationConfig:
    """Configuration for pagination request."""
    mode: PaginationMode = PaginationMode.OFFSET
    page: int = 1
    per_page: int = 50
    cursor: Optional[str] = None
    direction: str = "forward"  # "forward" or "backward"
    # Composite ordering - industry standard (Stripe, Twitter, GitHub)
    order_by: List[OrderColumn] = field(default_factory=lambda: [
        OrderColumn("id", "DESC")
    ])

@dataclass
class PaginatedResult:
    """Result of a paginated query."""
    data: List[Tuple]
    columns: List[str]
    page: int
    per_page: int
    total_count: Optional[int]  # None for cursor mode
    total_pages: Optional[int]  # None for cursor mode
    has_next: bool
    has_prev: bool
    next_cursor: Optional[str]  # Only for cursor mode
    prev_cursor: Optional[str]  # Only for cursor mode
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

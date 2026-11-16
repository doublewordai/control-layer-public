import { useCallback, useRef, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

function getDefaultPageSizeForScreen(): number {
  if (typeof window !== "undefined" && window.innerHeight >= 900) {
    return 20;
  }
  return 10;
}

interface UseServerCursorPaginationOptions {
  paramPrefix?: string;

  defaultPageSize?: number;
}

export function useServerCursorPagination(
  options: UseServerCursorPaginationOptions = {},
) {
  const { paramPrefix = "", defaultPageSize } = options;
  const [searchParams, setSearchParams] = useSearchParams();

  const [screenDefaultPageSize] = useState(() => getDefaultPageSizeForScreen());
  const effectiveDefaultPageSize = defaultPageSize ?? screenDefaultPageSize;

  const pageParam = paramPrefix ? `${paramPrefix}Page` : "page";
  const pageSizeParam = paramPrefix ? `${paramPrefix}PageSize` : "pageSize";
  const cursorParam = paramPrefix ? `${paramPrefix}After` : "after";

  const page = parseInt(searchParams.get(pageParam) || "1", 10);
  const pageSize = parseInt(
    searchParams.get(pageSizeParam) || String(effectiveDefaultPageSize),
    10,
  );
  const cursor = searchParams.get(cursorParam) || undefined;

  const cursorHistory = useRef<(string | undefined)[]>([]);

  const prevPageSize = useRef(pageSize);
  useEffect(() => {
    if (prevPageSize.current !== pageSize) {
      cursorHistory.current = [];
      prevPageSize.current = pageSize;
    }
  }, [pageSize]);

  const handleNextPage = useCallback(
    (lastItemId: string) => {
      cursorHistory.current[page - 1] = cursor;

      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set(pageParam, String(page + 1));
        next.set(cursorParam, lastItemId);
        return next;
      });
    },
    [cursor, page, pageParam, cursorParam, setSearchParams],
  );

  const handlePrevPage = useCallback(() => {
    if (page <= 1) return;

    const previousCursor = cursorHistory.current[page - 2];

    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set(pageParam, String(page - 1));

      if (previousCursor) {
        next.set(cursorParam, previousCursor);
      } else {
        next.delete(cursorParam);
      }

      return next;
    });
  }, [page, pageParam, cursorParam, setSearchParams]);

  const handleFirstPage = useCallback(() => {
    cursorHistory.current = [];

    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set(pageParam, "1");
      next.delete(cursorParam);
      return next;
    });
  }, [pageParam, cursorParam, setSearchParams]);

  const handlePageSizeChange = useCallback(
    (newPageSize: number) => {
      cursorHistory.current = [];

      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set(pageParam, "1");
        next.set(pageSizeParam, String(newPageSize));
        next.delete(cursorParam);
        return next;
      });
    },
    [pageParam, pageSizeParam, cursorParam, setSearchParams],
  );

  const hasPrevPage = page > 1;

  return {
    page,
    pageSize,
    cursor,

    handleNextPage,
    handlePrevPage,
    handleFirstPage,
    handlePageSizeChange,

    hasPrevPage,

    queryParams: {
      limit: pageSize + 1,
      ...(cursor && { after: cursor }),
    },
  };
}

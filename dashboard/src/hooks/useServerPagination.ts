import { useCallback, useState } from "react";
import { useSearchParams } from "react-router-dom";

function getDefaultPageSizeForScreen(): number {
  if (typeof window !== "undefined" && window.innerHeight >= 900) {
    return 20;
  }
  return 10;
}

interface UseServerPaginationOptions {
  paramPrefix?: string;

  defaultPageSize?: number;
}

export function useServerPagination(options: UseServerPaginationOptions = {}) {
  const { paramPrefix = "", defaultPageSize } = options;
  const [searchParams, setSearchParams] = useSearchParams();

  const [screenDefaultPageSize] = useState(() => getDefaultPageSizeForScreen());
  const effectiveDefaultPageSize = defaultPageSize ?? screenDefaultPageSize;

  const pageParam = paramPrefix ? `${paramPrefix}Page` : "page";
  const pageSizeParam = paramPrefix ? `${paramPrefix}PageSize` : "pageSize";

  const page = parseInt(searchParams.get(pageParam) || "1", 10);
  const pageSize = parseInt(
    searchParams.get(pageSizeParam) || String(effectiveDefaultPageSize),
    10,
  );

  const handlePageChange = useCallback(
    (newPage: number) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set(pageParam, String(newPage));
        return next;
      });
    },
    [pageParam, setSearchParams],
  );

  const handlePageSizeChange = useCallback(
    (newPageSize: number) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set(pageParam, "1");
        next.set(pageSizeParam, String(newPageSize));
        return next;
      });
    },
    [pageParam, pageSizeParam, setSearchParams],
  );

  const handleReset = useCallback(() => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set(pageParam, "1");
      return next;
    });
  }, [pageParam, setSearchParams]);

  const handleClear = useCallback(() => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.delete(pageParam);
      next.delete(pageSizeParam);
      return next;
    });
  }, [pageParam, pageSizeParam, setSearchParams]);

  const skip = (page - 1) * pageSize;

  return {
    page,
    pageSize,

    handlePageChange,
    handlePageSizeChange,
    handleReset,
    handleClear,

    queryParams: {
      skip,
      limit: pageSize,
    },
  };
}

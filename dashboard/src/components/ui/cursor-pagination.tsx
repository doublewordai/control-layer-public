import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "./pagination";

interface CursorPaginationProps {
  currentPage: number;
  itemsPerPage: number;
  onNextPage: () => void;
  onPrevPage: () => void;
  onFirstPage?: () => void;
  hasNextPage: boolean;
  hasPrevPage: boolean;
  currentPageItemCount: number;
  itemName?: string;
  className?: string;
}

export function CursorPagination({
  currentPage,
  itemsPerPage,
  onNextPage,
  onPrevPage,
  onFirstPage,
  hasNextPage,
  hasPrevPage,
  currentPageItemCount,
  itemName = "items",
  className = "",
}: CursorPaginationProps) {
  return (
    <>
      <Pagination className={`mt-8 ${className}`}>
        <PaginationContent>
          { }
          {onFirstPage && currentPage > 1 && (
            <PaginationItem>
              <PaginationLink
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onFirstPage();
                }}
              >
                First
              </PaginationLink>
            </PaginationItem>
          )}
          { }
          <PaginationItem>
            <PaginationPrevious
              href="#"
              onClick={(e) => {
                e.preventDefault();
                if (hasPrevPage) onPrevPage();
              }}
              className={
                !hasPrevPage
                  ? "pointer-events-none opacity-50"
                  : "cursor-pointer"
              }
            />
          </PaginationItem>

          { }
          <PaginationItem>
            <PaginationLink href="#" isActive>
              {currentPage}
            </PaginationLink>
          </PaginationItem>

          { }
          <PaginationItem>
            <PaginationNext
              href="#"
              onClick={(e) => {
                e.preventDefault();
                if (hasNextPage) onNextPage();
              }}
              className={
                !hasNextPage
                  ? "pointer-events-none opacity-50"
                  : "cursor-pointer"
              }
            />
          </PaginationItem>
        </PaginationContent>
      </Pagination>

      { }
      <div className="flex items-center justify-center mt-4 text-sm text-gray-600">
        Showing {itemsPerPage * (currentPage - 1) + 1}-
        {itemsPerPage * (currentPage - 1) + currentPageItemCount}
        {hasNextPage && " of many"} {itemName}
      </div>
    </>
  );
}

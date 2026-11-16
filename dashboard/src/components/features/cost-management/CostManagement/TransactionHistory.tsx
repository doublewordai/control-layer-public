import {
  TrendingDown,
  TrendingUp,
  X,
  Plus,
  DollarSign,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  Search,
  RefreshCw,
} from "lucide-react";
import { Card } from "../../../ui/card.tsx";
import { Button } from "@/components";
import { useState, useMemo, useCallback, type ReactNode } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useServerPagination } from "@/hooks/useServerPagination";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../../../ui/select.tsx";
import { DateTimeRangeSelector } from "../../../ui/date-time-range-selector.tsx";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../../ui/table.tsx";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { Transaction } from "@/api/control-layer";
import { useUserBalance, useTransactions, useUser } from "@/api/control-layer";
import { useSettings } from "@/contexts";
import { formatDollars } from "@/utils/money.ts";
import { getServiceTierLabel } from "@/utils/serviceTier";
import { useDebounce } from "@/hooks/useDebounce";

export type AddFundsConfig =
  | { type: "admin-only"; onGiftFunds: () => void }
  | { type: "purchase-only"; onPurchaseFunds: () => void }
  | {
      type: "split";
      onPurchaseFunds: () => void;
      onGiftFunds?: () => void;
      onBillingPortal?: () => void;
    }
  | undefined;

export interface TransactionHistoryProps {
  userId: string;
  showCard?: boolean;
  addFundsConfig?: AddFundsConfig;
  filterUserId?: string;
  headerExtra?: ReactNode;
}

export function TransactionHistory({
  userId,
  showCard = true,
  addFundsConfig,
  filterUserId,
  headerExtra,
}: TransactionHistoryProps) {
  const { isFeatureEnabled } = useSettings();
  const isDemoMode = isFeatureEnabled("demo");
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const { data: displayUser } = useUser(filterUserId || userId);

  const pagination = useServerPagination({ paramPrefix: "tx" });

  const transactionType = searchParams.get("txType") || "all";

  const [dateRange, setDateRange] = useState<
    { from: Date; to: Date } | undefined
  >();
  const [searchTerm, setSearchTerm] = useState<string>("");
  const debouncedSearch = useDebounce(searchTerm, 300);

  const { refetch: refetchBalance } = useUserBalance(userId);

  const getTransactionTypesParam = (type: string): string | undefined => {
    if (type === "credit") return "admin_grant,purchase";
    if (type === "debit") return "admin_removal,usage";
    return undefined;
  };

  const {
    data: transactionsResponse,
    isLoading: isLoadingTransactions,
    refetch: refetchTransactions,
  } = useTransactions({
    userId,
    group_batches: true,
    ...pagination.queryParams,
    search: debouncedSearch.trim() || undefined,
    transaction_types: getTransactionTypesParam(transactionType),
    start_date: dateRange?.from?.toISOString(),
    end_date: dateRange?.to?.toISOString(),
  });

  const transactions = useMemo<Transaction[]>(() => {
    return transactionsResponse?.data || [];
  }, [transactionsResponse]);

  const hasMore = transactionsResponse?.has_more ?? false;
  const pageStartBalance = transactionsResponse?.page_start_balance ?? 0;

  const balanceByTransactionId = useMemo(() => {
    const balanceMap = new Map<string, number>();
    let runningBalance = Number(pageStartBalance);

    for (const tx of transactions) {
      balanceMap.set(tx.id, runningBalance);

      const amount = Number(tx.amount);
      const isCredit =
        tx.transaction_type === "admin_grant" ||
        tx.transaction_type === "purchase";
      if (isCredit) {
        runningBalance -= amount;
      } else {
        runningBalance += amount;
      }
    }

    return balanceMap;
  }, [transactions, pageStartBalance]);

  const isLoading = !isDemoMode && isLoadingTransactions;

  const formatDate = (isoString: string) => {
    const date = new Date(isoString);
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  };

  const formatDescription = (tx: Transaction): string => {
    const baseDescription = tx.description || "No description";

    if (tx.transaction_type !== "usage") {
      return baseDescription;
    }

    const tierKey = tx.service_tier ?? (tx.batch_id ? "batch" : "realtime");
    const tierLabel = getServiceTierLabel(tierKey);
    if (tx.batch_id) {
      const requestCount = tx.batch_request_count || 0;
      const requestsText = requestCount > 0 ? `: ${requestCount} requests` : "";
      return `${tierLabel}${requestsText}`;
    }

    const match = baseDescription.match(/^API usage:\s*(.+?)\s*\(/i);
    const model = match ? match[1] : baseDescription.replace(/^API usage:\s*/i, "");

    return `${tierLabel}: ${model}`;
  };

  const hasActiveFilters =
    transactionType !== "all" || dateRange !== undefined || searchTerm !== "";

  const updateUrlParamsAndResetPage = useCallback(
    (updates: Record<string, string | null>) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        for (const [key, value] of Object.entries(updates)) {
          if (value === null) {
            next.delete(key);
          } else {
            next.set(key, value);
          }
        }
        next.set("txPage", "1");
        return next;
      });
    },
    [setSearchParams],
  );

  const clearFilters = useCallback(() => {
    setDateRange(undefined);
    setSearchTerm("");
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.delete("txType");
      next.set("txPage", "1");
      return next;
    });
  }, [setSearchParams]);

  const handleTransactionTypeChange = useCallback(
    (value: string) => {
      updateUrlParamsAndResetPage({ txType: value === "all" ? null : value });
    },
    [updateUrlParamsAndResetPage],
  );

  const handleDateRangeChange = useCallback(
    (range: { from: Date; to: Date } | undefined) => {
      setDateRange(range);
      pagination.handleReset();
    },
    [pagination],
  );

  const handleSearchChange = useCallback(
    (value: string) => {
      setSearchTerm(value);
      pagination.handleReset();
    },
    [pagination],
  );

  const [isRefreshing, setIsRefreshing] = useState(false);
  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      await Promise.all([refetchBalance(), refetchTransactions()]);
    } finally {
      setIsRefreshing(false);
    }
  };

  const content = (
    <>
      { }
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-semibold text-doubleword-neutral-900">
              Transaction History
            </h2>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleRefresh}
              disabled={isRefreshing || isLoading}
              className="h-8 w-8 p-0"
              title="Refresh balance and transactions"
            >
              <RefreshCw
                className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`}
              />
            </Button>
          </div>
          {displayUser && (
            <p
              className={`text-sm mt-1 ${filterUserId && filterUserId !== userId ? "text-red-600 font-semibold" : "text-gray-600"}`}
            >
              Showing transactions for{" "}
              {displayUser.user_type === "organization" ? (
                <span className="font-medium">
                  {displayUser.display_name || displayUser.username}
                </span>
              ) : (
                <>
                  user{" "}
                  <span className="font-medium">{displayUser.email}</span>
                </>
              )}
            </p>
          )}
        </div>
        <div className="flex items-center gap-6">
          {headerExtra}
          {addFundsConfig && (
            <>
              {addFundsConfig.type === "admin-only" && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={addFundsConfig.onGiftFunds}
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Gift/Remove Funds
                </Button>
              )}
              {addFundsConfig.type === "purchase-only" && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={addFundsConfig.onPurchaseFunds}
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add to Credit Balance
                </Button>
              )}
              {addFundsConfig.type === "split" && (
                <div className="flex">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={addFundsConfig.onPurchaseFunds}
                    className="rounded-r-none"
                  >
                    <Plus className="w-4 h-4 mr-2" />
                    Add to Credit Balance
                  </Button>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="outline"
                        size="sm"
                        className="rounded-l-none border-l-0 px-2"
                      >
                        <ChevronDown className="w-4 h-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      {addFundsConfig.onGiftFunds && (
                        <DropdownMenuItem onClick={addFundsConfig.onGiftFunds}>
                          Gift/Remove Funds
                        </DropdownMenuItem>
                      )}
                      {addFundsConfig.onBillingPortal && (
                        <DropdownMenuItem
                          onClick={addFundsConfig.onBillingPortal}
                        >
                          Billing Portal
                        </DropdownMenuItem>
                      )}
                    </DropdownMenuContent>
                  </DropdownMenu>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      { }
      <Card className="p-4">
        { }
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            { }
            <div className="relative flex-1 min-w-[250px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-doubleword-neutral-400" />
              <input
                type="text"
                placeholder="Search transactions..."
                value={searchTerm}
                onChange={(e) => handleSearchChange(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-doubleword-neutral-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>

            { }
            <Select
              value={transactionType}
              onValueChange={handleTransactionTypeChange}
            >
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="All types" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All types</SelectItem>
                <SelectItem value="credit">Deposits only</SelectItem>
                <SelectItem value="debit">Withdrawals only</SelectItem>
              </SelectContent>
            </Select>

            { }
            <DateTimeRangeSelector
              value={dateRange}
              onChange={handleDateRangeChange}
            />

            { }
            <Select
              value={pagination.pageSize.toString()}
              onValueChange={(value) =>
                pagination.handlePageSizeChange(parseInt(value, 10))
              }
            >
              <SelectTrigger className="w-[80px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="10">10</SelectItem>
                <SelectItem value="20">20</SelectItem>
                <SelectItem value="50">50</SelectItem>
                <SelectItem value="100">100</SelectItem>
              </SelectContent>
            </Select>

            <Button
              variant="ghost"
              size="sm"
              onClick={clearFilters}
              disabled={!hasActiveFilters}
              className="h-9"
            >
              <X className="w-4 h-4 mr-1" />
              Clear
            </Button>
          </div>

          { }
          {hasActiveFilters && (
            <div className="text-sm text-doubleword-neutral-600">
              Showing filtered transactions (page {pagination.page})
            </div>
          )}
        </div>

        <div className="-mt-2 mb-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[50px]"></TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Date</TableHead>
                <TableHead className="text-right">Amount</TableHead>
                <TableHead
                  className={`text-right ${debouncedSearch || transactionType !== "all" ? "hidden" : ""}`}
                >
                  Balance
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {transactions.map((transaction) => {
                const isCredit =
                  transaction.transaction_type === "admin_grant" ||
                  transaction.transaction_type === "purchase";

                const handleRowClick = () => {
                  if (transaction.batch_id) {
                    const currentParams = searchParams.toString();
                    const fromUrl = currentParams
                      ? `/cost-management?${currentParams}`
                      : "/cost-management";
                    navigate(
                      `/batches/${transaction.batch_id}?from=${encodeURIComponent(fromUrl)}`,
                    );
                  }
                };

                return (
                  <TableRow
                    key={transaction.id}
                    onClick={handleRowClick}
                    tabIndex={transaction.batch_id ? 0 : -1}
                    onKeyDown={(event) => {
                      if (!transaction.batch_id) return;
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        handleRowClick();
                      }
                    }}
                    role={transaction.batch_id ? "button" : undefined}
                    aria-label={
                      transaction.batch_id
                        ? `View batch ${transaction.batch_id} details`
                        : undefined
                    }
                    className={
                      transaction.batch_id
                        ? "cursor-pointer hover:bg-doubleword-neutral-50"
                        : ""
                    }
                  >
                    <TableCell>
                      <div
                        className={`p-2 rounded-full ${
                          isCredit ? "bg-green-100" : "bg-red-100"
                        }`}
                      >
                        {isCredit ? (
                          <TrendingUp className="w-4 h-4 text-green-600" />
                        ) : (
                          <TrendingDown className="w-4 h-4 text-red-600" />
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <p className="font-medium text-doubleword-neutral-900">
                        {formatDescription(transaction)}
                      </p>
                    </TableCell>
                    <TableCell>
                      <p className="text-sm text-doubleword-neutral-600">
                        {formatDate(transaction.created_at)}
                      </p>
                    </TableCell>
                    <TableCell className="text-right">
                      <p
                        className={`font-semibold ${
                          isCredit ? "text-green-600" : "text-red-600"
                        }`}
                      >
                        {isCredit ? "+" : "-"}
                        {formatDollars(transaction.amount, 9)}
                      </p>
                    </TableCell>
                    <TableCell
                      className={`text-right ${debouncedSearch || transactionType !== "all" ? "hidden" : ""}`}
                    >
                      <p className="text-sm text-doubleword-neutral-600">
                        {formatDollars(
                          balanceByTransactionId.get(transaction.id) ?? 0,
                        )}
                      </p>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>

        {transactions.length === 0 && (
          <div className="text-center py-8">
            <DollarSign className="w-12 h-12 text-doubleword-neutral-300 mx-auto mb-3" />
            <p className="text-doubleword-neutral-600">
              {hasActiveFilters
                ? "No transactions match your filters"
                : "No transactions yet"}
            </p>
            {hasActiveFilters && (
              <Button
                variant="outline"
                size="sm"
                onClick={clearFilters}
                className="mt-4"
              >
                Clear filters
              </Button>
            )}
          </div>
        )}

        { }
        {(transactions.length > 0 || pagination.page > 1) && (
          <div className="flex items-center justify-between border-t border-doubleword-neutral-200 pt-2">
            <div className="text-sm text-doubleword-neutral-600">
              {transactions.length > 0 ? (
                <>
                  Showing {pagination.queryParams.skip + 1} to{" "}
                  {pagination.queryParams.skip + transactions.length}{" "}
                  transactions
                </>
              ) : (
                <>No transactions on this page</>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => pagination.handlePageChange(1)}
                disabled={pagination.page === 1}
              >
                First
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => pagination.handlePageChange(pagination.page - 1)}
                disabled={pagination.page === 1}
              >
                <ChevronLeft className="w-4 h-4" />
                Previous
              </Button>
              <div className="text-sm text-doubleword-neutral-600">
                Page {pagination.page}
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => pagination.handlePageChange(pagination.page + 1)}
                disabled={!hasMore}
              >
                Next
                <ChevronRight className="w-4 h-4" />
              </Button>
            </div>
          </div>
        )}
      </Card>
    </>
  );

  if (isLoading) {
    return (
      <Card className="p-8 text-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-doubleword-accent-blue mx-auto mb-4"></div>
        <p className="text-doubleword-neutral-600">Loading transactions...</p>
      </Card>
    );
  }

  if (showCard) {
    return <Card className="p-6">{content}</Card>;
  }

  return <div>{content}</div>;
}

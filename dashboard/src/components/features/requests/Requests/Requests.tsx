import { Activity, X, List, ArrowLeft, LayoutGrid } from "lucide-react";
import { useState, useEffect } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import {
  useRequests,
  useRequestsAggregate,
} from "../../../../api/control-layer";
import type { AnalyticsEntry } from "../../../../api/control-layer/types";
import { useAuthorization } from "../../../../utils/authorization";
import { useServerPagination } from "../../../../hooks/useServerPagination";
import { DataTable } from "../../../ui/data-table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../../../ui/tabs";
import { Combobox } from "../../../ui/combobox";
import { DateTimeRangeSelector } from "../../../ui/date-time-range-selector";
import { RequestsAnalytics } from "../RequestsAnalytics";
import { createRequestColumns } from "./columns";
import type { RequestsEntry } from "../types";
import { useDebounce } from "../../../../hooks/useDebounce";

function transformAnalyticsEntry(entry: AnalyticsEntry): RequestsEntry {
  return {
    id: String(entry.id),
    timestamp: entry.timestamp,
    method: entry.method,
    uri: entry.uri,
    model: entry.model,
    status_code: entry.status_code,
    duration_ms: entry.duration_ms,
    prompt_tokens: entry.prompt_tokens,
    completion_tokens: entry.completion_tokens,
    reasoning_tokens: entry.reasoning_tokens,
    total_tokens: entry.total_tokens,
    response_type: entry.response_type,
    fusillade_batch_id: entry.fusillade_batch_id,
    input_price_per_token: entry.input_price_per_token,
    output_price_per_token: entry.output_price_per_token,
    custom_id: entry.custom_id,
  };
}

export function Requests() {
  const { userRoles } = useAuthorization();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const [selectedModel, setSelectedModel] = useState<string | undefined>(
    undefined,
  );
  const [batchIdFilter, setBatchIdFilter] = useState<string | undefined>(
    undefined,
  );
  const [customIdSearch, setCustomIdSearch] = useState<string>("");
  const debouncedCustomIdSearch = useDebounce(customIdSearch, 300);

  const getDefaultDateRange = () => {
    const now = new Date();
    const from = new Date(now.getTime() - 31 * 24 * 60 * 60 * 1000);
    return { from, to: now };
  };
  const [dateRange, setDateRange] = useState<
    { from: Date; to: Date } | undefined
  >(getDefaultDateRange());

  const pagination = useServerPagination({});

  useEffect(() => {
    pagination.handlePageChange(1);
    /* eslint-disable-next-line */
  }, [debouncedCustomIdSearch]);

  const queryLimit = pagination.pageSize + 1;

  const hasAnalyticsPermission = userRoles.some(
    (role) => role === "PlatformManager" || role === "RequestViewer",
  );
  const hasRequestsPermission = userRoles.some(
    (role) => role === "RequestViewer",
  );

  useEffect(() => {
    const modelFromUrl = searchParams.get("model");
    if (modelFromUrl) {
      setSelectedModel(modelFromUrl);
    }
    const batchIdFromUrl = searchParams.get("fusillade_batch_id");
    if (batchIdFromUrl) {
      setBatchIdFilter(batchIdFromUrl);
    }
  }, [searchParams]);

  useEffect(() => {
    const tabFromUrl = searchParams.get("tab");
    if (
      tabFromUrl &&
      (tabFromUrl === "analytics" || tabFromUrl === "requests")
    ) {
      setActiveTab(tabFromUrl);
    }
  }, [searchParams]);

  const tabFromUrl = searchParams.get("tab");
  const [activeTab, setActiveTab] = useState<string>(() => {
    return tabFromUrl &&
      (tabFromUrl === "analytics" || tabFromUrl === "requests")
      ? tabFromUrl
      : hasAnalyticsPermission
        ? "analytics"
        : "requests";
  });

  const handleTabChange = (value: string) => {
    setActiveTab(value);
    const newParams = new URLSearchParams(searchParams);
    newParams.set("tab", value);
    navigate(`/analytics?${newParams.toString()}`, { replace: true });
  };

  const fromUrl = searchParams.get("from");

  const {
    data: requestsResponse,
    isLoading: requestsLoading,
    error: requestsError,
  } = useRequests(
    {
      skip: pagination.queryParams.skip,
      limit: queryLimit,
      order_desc: true,
      model: selectedModel,
      fusillade_batch_id: batchIdFilter,
      custom_id: debouncedCustomIdSearch || undefined,
    },
    { enabled: hasRequestsPermission && activeTab === "requests" },
    dateRange,
  );

  const { data: analyticsData } = useRequestsAggregate(undefined, dateRange);
  const modelsWithRequests = analyticsData?.models || [];
  const error = requestsError;

  const allRequestsRaw = requestsResponse?.entries
    ? requestsResponse.entries.map(transformAnalyticsEntry)
    : [];

  const hasMore = allRequestsRaw.length > pagination.pageSize;

  const requests = hasMore
    ? allRequestsRaw.slice(0, pagination.pageSize)
    : allRequestsRaw;

  const hasPrevPage = pagination.page > 1;
  const hasNextPage = hasMore;

  const columns = createRequestColumns();

  if (error) {
    return (
      <div className="py-4 px-6">
        <div className="mb-4">
          <h1 className="text-3xl font-bold text-doubleword-neutral-900">
            Analytics
          </h1>
        </div>
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="text-red-500 mb-4">
              <X className="h-12 w-12 mx-auto" />
            </div>
            <h3 className="text-lg font-medium text-red-600 mb-2">
              Error Loading Data
            </h3>
            <p className="text-red-600">
              {error instanceof Error
                ? error.message
                : "Failed to load request data"}
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (!hasAnalyticsPermission && !hasRequestsPermission) {
    return (
      <div className="py-4 px-6">
        <div className="mb-4">
          <h1 className="text-3xl font-bold text-doubleword-neutral-900">
            Analytics
          </h1>
        </div>
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="text-red-500 mb-4">
              <X className="h-12 w-12 mx-auto" />
            </div>
            <h3 className="text-lg font-medium text-red-600 mb-2">
              Access Denied
            </h3>
            <p className="text-red-600">
              You don't have permission to view analytics data.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="py-4 px-6">
      <Tabs
        value={activeTab}
        onValueChange={handleTabChange}
        className="space-y-4"
      >
        <div className="mb-4 flex flex-col sm:flex-row sm:items-end sm:justify-between gap-4">
          <div className="flex items-center gap-4">
            {fromUrl && (
              <button
                onClick={() => navigate(fromUrl)}
                className="p-2 text-gray-500 hover:bg-gray-100 rounded-lg transition-colors"
                aria-label="Go back"
                title="Go back"
              >
                <ArrowLeft className="w-5 h-5" />
              </button>
            )}
            <div>
              <h1 className="text-3xl font-bold text-doubleword-neutral-900">
                {`${activeTab.slice(0, 1).toUpperCase()}${activeTab.slice(1)}`}
              </h1>
              {batchIdFilter && (
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-sm text-doubleword-neutral-600">
                    Filtered by batch:
                  </span>
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                    <span className="font-mono">
                      {batchIdFilter.slice(0, 8)}...
                    </span>
                    <button
                      onClick={() => {
                        setBatchIdFilter(undefined);
                        const newParams = new URLSearchParams(searchParams);
                        newParams.delete("fusillade_batch_id");
                        navigate(`/analytics?${newParams.toString()}`, {
                          replace: true,
                        });
                      }}
                      className="ml-1 hover:text-blue-900"
                      aria-label="Clear batch filter"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </span>
                </div>
              )}
            </div>
          </div>

          <div className="flex flex-col sm:flex-row items-start sm:items-center gap-4">
            <DateTimeRangeSelector
              value={dateRange}
              onChange={setDateRange}
              className="w-full sm:w-auto"
            />

            <Combobox
              options={[
                { value: "all", label: "All Models" },
                ...modelsWithRequests.map((model) => ({
                  value: model.model,
                  label: model.model,
                })),
              ]}
              value={selectedModel || "all"}
              onValueChange={(value) => {
                const newModel = value === "all" ? undefined : value;
                setSelectedModel(newModel);

                const newParams = new URLSearchParams(searchParams);
                if (newModel) {
                  newParams.set("model", newModel);
                } else {
                  newParams.delete("model");
                }
                navigate(`/analytics?${newParams.toString()}`, {
                  replace: true,
                });
              }}
              placeholder="Select model..."
              searchPlaceholder="Search models..."
              emptyMessage="No models with requests found."
              className="w-full sm:w-[200px]"
            />

            <TabsList className="w-full sm:w-auto">
              {hasAnalyticsPermission && (
                <TabsTrigger
                  value="analytics"
                  className="flex items-center gap-2 flex-1 sm:flex-initial"
                >
                  <LayoutGrid className="h-4 w-4" />
                  Dashboard
                </TabsTrigger>
              )}
              {hasRequestsPermission && (
                <TabsTrigger
                  value="requests"
                  className="flex items-center gap-2 flex-1 sm:flex-initial"
                >
                  <List className="h-4 w-4" />
                  Requests
                </TabsTrigger>
              )}
            </TabsList>
          </div>
        </div>

        {hasRequestsPermission && (
          <TabsContent value="requests" className="space-y-4">
            <DataTable
              columns={columns}
              data={requests}
              searchPlaceholder="Search by custom ID..."
              externalSearch={{
                value: customIdSearch,
                onChange: setCustomIdSearch,
              }}
              showColumnToggle={true}
              pageSize={pagination.pageSize}
              minRows={pagination.pageSize}
              rowHeight="40px"
              initialColumnVisibility={{ timestamp: false }}
              paginationMode="server-cursor"
              serverPagination={{
                page: pagination.page,
                pageSize: pagination.pageSize,
                onNextPage: () =>
                  pagination.handlePageChange(pagination.page + 1),
                onPrevPage: () =>
                  pagination.handlePageChange(pagination.page - 1),
                onFirstPage: () => pagination.handlePageChange(1),
                onPageSizeChange: pagination.handlePageSizeChange,
                hasNextPage: hasNextPage,
                hasPrevPage: hasPrevPage,
              }}
              showPageSizeSelector={true}
              pageSizeOptions={[10, 20, 50, 100]}
              isLoading={requestsLoading}
              emptyState={
                <div className="text-center py-12">
                  <div className="p-4 bg-doubleword-neutral-100 rounded-full w-16 h-16 mx-auto mb-4 flex items-center justify-center">
                    <Activity className="w-8 h-8 text-doubleword-neutral-600" />
                  </div>
                  <h3 className="text-lg font-medium text-doubleword-neutral-900 mb-2">
                    No requests found
                  </h3>
                  <p className="text-doubleword-neutral-600">
                    {debouncedCustomIdSearch
                      ? "No requests match your search. Try a different custom ID."
                      : "No requests found for the selected time period. Try adjusting the date range or check back later once traffic starts flowing through the gateway."}
                  </p>
                </div>
              }
            />
          </TabsContent>
        )}

        {hasAnalyticsPermission && (
          <TabsContent value="analytics" className="space-y-4">
            <RequestsAnalytics
              selectedModel={selectedModel}
              dateRange={dateRange}
            />
          </TabsContent>
        )}
      </Tabs>
    </div>
  );
}

import React, { useState, useMemo } from "react";
import type { ProbeResult } from "../../../../api/control-layer";

interface TimeWindow {
  startTime: Date;
  endTime: Date;
  results: ProbeResult[];
  successCount: number;
  failureCount: number;
  avgResponseTime: number | null;
}

interface ProbeTimelineProps {
  results: ProbeResult[];
  compact?: boolean;
  showSummary?: boolean;
  showTimeLabels?: boolean;
  showLegend?: boolean;
}

export const ProbeTimeline: React.FC<ProbeTimelineProps> = ({
  results,
  compact = false,
  showSummary = true,
  showTimeLabels = true,
  showLegend = true,
}) => {
  const [hoveredWindow, setHoveredWindow] = useState<TimeWindow | null>(null);
  const [hoverPosition, setHoverPosition] = useState({ x: 0, y: 0 });

  const windows = useMemo(() => {
    const WINDOW_SIZE_MS = 5 * 60 * 1000;
    const LOOKBACK_HOURS = 4;
    const windows: TimeWindow[] = [];

    const endTime = new Date();
    const startTime = new Date(
      endTime.getTime() - LOOKBACK_HOURS * 60 * 60 * 1000,
    );

    let currentTime = new Date(
      Math.floor(startTime.getTime() / WINDOW_SIZE_MS) * WINDOW_SIZE_MS,
    );

    const sortedResults = results
      ? [...results].sort(
          (a, b) =>
            new Date(a.executed_at).getTime() -
            new Date(b.executed_at).getTime(),
        )
      : [];

    while (currentTime < endTime) {
      const windowStart = new Date(currentTime);
      const windowEnd = new Date(currentTime.getTime() + WINDOW_SIZE_MS);

      const windowResults = sortedResults.filter((r) => {
        const resultTime = new Date(r.executed_at);
        return resultTime >= windowStart && resultTime < windowEnd;
      });

      const successCount = windowResults.filter((r) => r.success).length;
      const failureCount = windowResults.filter((r) => !r.success).length;
      const responseTimes = windowResults
        .filter((r) => r.success && r.response_time_ms)
        .map((r) => r.response_time_ms!);
      const avgResponseTime =
        responseTimes.length > 0
          ? responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length
          : null;

      windows.push({
        startTime: windowStart,
        endTime: windowEnd,
        results: windowResults,
        successCount,
        failureCount,
        avgResponseTime,
      });

      currentTime = windowEnd;
    }

    return windows;
  }, [results]);

  const totalChecks = results.length;
  const totalSuccess = results.filter((r) => r.success).length;
  const totalFailure = results.filter((r) => !r.success).length;
  const successRate =
    totalChecks > 0 ? ((totalSuccess / totalChecks) * 100).toFixed(1) : "0.0";

  const handleMouseEnter = (window: TimeWindow, event: React.MouseEvent) => {
    const rect = event.currentTarget.getBoundingClientRect();
    setHoverPosition({ x: rect.left + rect.width / 2, y: rect.top });
    setHoveredWindow(window);
  };

  const handleMouseLeave = () => {
    setHoveredWindow(null);
  };


  return (
    <div className="relative">
      { }
      {showSummary && (
        <div className="flex items-center gap-6 mb-2 text-sm">
          <div>
            <span className="text-gray-600">Last {totalChecks} checks:</span>
            <span className="ml-2 font-semibold text-green-600">
              {totalSuccess} up
            </span>
            {totalFailure > 0 && (
              <span className="ml-2 font-semibold text-red-600">
                {totalFailure} down
              </span>
            )}
            <span className="ml-2 text-gray-500">({successRate}% uptime)</span>
          </div>
        </div>
      )}

      { }
      <div className="space-y-1">
        <div
          className={`flex gap-0.5 items-center ${compact ? "h-10" : "h-12"}`}
        >
          {windows.map((window, index) => {
            if (window.results.length === 0) {
              return (
                <div
                  key={index}
                  className="min-w-[2px] flex-1 h-full border border-gray-300 rounded cursor-pointer hover:border-gray-400 transition-colors"
                  onMouseEnter={(e) => handleMouseEnter(window, e)}
                  onMouseLeave={handleMouseLeave}
                />
              );
            }

            let colorClass = "bg-green-500";
            const windowSuccessRate =
              window.successCount / window.results.length;
            if (windowSuccessRate === 0) {
              colorClass = "bg-red-500";
            } else if (windowSuccessRate < 1) {
              colorClass = "bg-yellow-500";
            }

            return (
              <div
                key={index}
                className={`min-w-[2px] flex-1 h-full ${colorClass} rounded cursor-pointer hover:opacity-80 transition-opacity`}
                onMouseEnter={(e) => handleMouseEnter(window, e)}
                onMouseLeave={handleMouseLeave}
              />
            );
          })}
        </div>

        { }
        {showTimeLabels && windows.length > 0 && (
          <div className="flex justify-between text-xs text-gray-500">
            <span>{windows[0].startTime.toLocaleTimeString()}</span>
            <span>
              {windows[windows.length - 1].endTime.toLocaleTimeString()}
            </span>
          </div>
        )}
      </div>

      { }
      {showLegend && (
        <div className="flex gap-6 text-sm text-gray-600 mt-4 pt-4 border-t">
          <div className="flex items-center gap-2">
            <span className="w-4 h-4 bg-green-500 rounded"></span>
            <span>Up (100%)</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-4 h-4 bg-yellow-500 rounded"></span>
            <span>Degraded (1-99%)</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-4 h-4 bg-red-500 rounded"></span>
            <span>Down (0%)</span>
          </div>
        </div>
      )}

      { }
      {hoveredWindow && (
        <div
          className="fixed z-50 bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-sm pointer-events-none min-w-[180px]"
          style={{
            left: `${hoverPosition.x}px`,
            top: `${hoverPosition.y - 10}px`,
            transform: "translate(-50%, -100%)",
          }}
        >
          <div className="font-semibold mb-1 whitespace-nowrap">
            {hoveredWindow.startTime.toLocaleTimeString()} -{" "}
            {hoveredWindow.endTime.toLocaleTimeString()}
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <span className="text-green-600">
                ✓ {hoveredWindow.successCount}
              </span>
              <span className="text-red-600">
                ✗ {hoveredWindow.failureCount}
              </span>
            </div>
            <div className="text-gray-600">
              Avg:{" "}
              {hoveredWindow.avgResponseTime
                ? `${Math.round(hoveredWindow.avgResponseTime)}ms`
                : "N/A"}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

import React from "react";

interface SparklineProps {
  data: { timestamp: string; requests: number }[];
  width?: number;
  height?: number;
  className?: string;
}

export const Sparkline: React.FC<SparklineProps> = ({
  data,
  width = 120,
  height = 40,
  className = "",
}) => {
  if (!data || data.length === 0) {
    return (
      <svg
        width={width}
        height={height}
        className={`text-gray-300 ${className}`}
        aria-label="No activity data"
      >
        <defs>
          <linearGradient id="noDataGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="currentColor" stopOpacity="0.3" />
            <stop offset="100%" stopColor="currentColor" stopOpacity="0.1" />
          </linearGradient>
        </defs>
        <rect
          x="0"
          y={height / 2 - 1}
          width={width}
          height="2"
          fill="url(#noDataGradient)"
          rx="1"
        />
        <text
          x={width / 2}
          y={height / 2 + 1}
          textAnchor="middle"
          className="text-xs fill-current opacity-50"
          dominantBaseline="middle"
        >
          No data
        </text>
      </svg>
    );
  }

  const values = data.map((d) => d.requests);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = max - min || 1;

  const marginTop = 2;
  const marginBottom = 4;
  const chartHeight = height - marginTop - marginBottom;
  const chartWidth = width;

  const pathPoints = data.map((point, index) => {
    const x = (index / Math.max(data.length - 1, 1)) * chartWidth;
    const y =
      marginTop + chartHeight - ((point.requests - min) / range) * chartHeight;
    return { x, y };
  });

  const createSteppedPath = (
    points: { x: number; y: number }[],
    isArea = false,
  ) => {
    if (points.length === 0) return "";
    if (points.length === 1) {
      const point = points[0];
      return isArea
        ? `M 0 ${height} L ${point.x} ${point.y} L ${width} ${height} Z`
        : `M ${point.x} ${point.y}`;
    }

    let path = isArea
      ? `M 0 ${height} L ${points[0].x} ${points[0].y}`
      : `M ${points[0].x} ${points[0].y}`;

    for (let i = 1; i < points.length; i++) {
      const current = points[i];
      const prev = points[i - 1];

      path += ` L ${current.x} ${prev.y}`;
      path += ` L ${current.x} ${current.y}`;
    }

    if (isArea) {
      path += ` L ${width} ${height} Z`;
    }

    return path;
  };

  const allZero = values.every((v) => v === 0);

  const areaPath = createSteppedPath(pathPoints, true);

  const linePath = allZero
    ? `M 0 ${pathPoints[0]?.y || height - marginBottom} L ${width} ${pathPoints[0]?.y || height - marginBottom}`
    : createSteppedPath(pathPoints, false);

  const startTime = new Date(data[0].timestamp);
  const formatTime = (date: Date) =>
    date.toLocaleTimeString("en-US", {
      hour: "numeric",
      hour12: true,
    });

  return (
    <div className="flex flex-col items-center gap-0.5">
      <svg
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        className={`${className}`}
        preserveAspectRatio="xMidYMid meet"
        aria-label={`Activity chart showing ${data.length} data points over 24 hours`}
      >
        <defs>
          <linearGradient id="areaGradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgb(59, 130, 246)" stopOpacity="0.3" />
            <stop
              offset="100%"
              stopColor="rgb(59, 130, 246)"
              stopOpacity="0.05"
            />
          </linearGradient>
          <filter id="glow" filterUnits="userSpaceOnUse">
            <feGaussianBlur stdDeviation="1" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        { }
        <path d={areaPath} fill="url(#areaGradient)" />

        { }
        <path
          d={linePath}
          fill="none"
          stroke="rgb(59, 130, 246)"
          strokeWidth="1"
          filter="url(#glow)"
          vectorEffect="non-scaling-stroke"
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        { }
        {pathPoints.map((point, index) => (
          <circle
            key={index}
            cx={point.x}
            cy={point.y}
            r="2"
            fill="rgb(59, 130, 246)"
            stroke="white"
            strokeWidth="0.5"
            opacity="0.8"
          />
        ))}
      </svg>

      { }
      <div className="flex justify-between w-full text-xs text-gray-400">
        <span>{formatTime(startTime)}</span>
        <span>now</span>
      </div>
    </div>
  );
};

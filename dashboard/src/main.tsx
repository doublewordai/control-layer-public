import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/space-grotesk/300.css";
import "@fontsource/space-grotesk/400.css";
import "@fontsource/space-grotesk/500.css";
import "@fontsource/space-grotesk/600.css";
import "@fontsource/space-grotesk/700.css";
import "./index.css";
import App from "./App.tsx";
import { captureException, setTelemetryContext } from "./lib/telemetry";

setTelemetryContext({
  build_sha: import.meta.env.VITE_BUILD_SHA ?? "unknown",
  environment: import.meta.env.MODE,
});

window.addEventListener("error", (event) => {
  const error =
    event.error ??
    new Error(event.message || "Unknown window error");
  captureException(error, {
    source: "window-error",
    context: {
      filename: event.filename,
      line: event.lineno,
      column: event.colno,
    },
  });
});

window.addEventListener("unhandledrejection", (event) => {
  captureException(event.reason, { source: "unhandled-rejection" });
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

import { http, HttpResponse } from "msw";
import requestsData from "./data/requests.json";

export const demoHandlers = [
  http.get("/api/v1/requests", ({ request }) => {
    const url = new URL(request.url);
    const limit = url.searchParams.get("limit");
    const offset = url.searchParams.get("offset");
    const userId = url.searchParams.get("user_id");
    const model = url.searchParams.get("model");

    let filteredRequests = [...requestsData];

    if (userId) {
      filteredRequests = filteredRequests.filter(
        (req) => req.metadata.email === userId,
      );
    }
    if (model) {
      filteredRequests = filteredRequests.filter((req) => req.model === model);
    }

    const startIndex = offset ? parseInt(offset, 10) : 0;
    const endIndex = limit ? startIndex + parseInt(limit, 10) : undefined;

    const paginatedRequests = filteredRequests.slice(startIndex, endIndex);

    return HttpResponse.json(paginatedRequests);
  }),
];

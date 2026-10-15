import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

/**
 * Submits runs against anchor-server as fast as the configured arrival rate.
 *
 * Measures ANCHOR, not a model. The worker must be started with the mock
 * provider at a fixed latency, so every number here describes queueing,
 * claiming, journaling and projection cost. Pointed at a real provider this
 * would mostly measure Anthropic's response time, which is not the thing being
 * reported.
 *
 *   k6 run -e ANCHOR_URL=http://localhost:8080 -e AGENT=bench loadtest/submit.js
 */

const submitted = new Counter("anchor_submitted");
const rejected = new Counter("anchor_rejected_429");
const submitLatency = new Trend("anchor_submit_latency_ms", true);

const baseUrl = __ENV.ANCHOR_URL || "http://localhost:8080";
const agent = __ENV.AGENT || "bench";
const apiKey = __ENV.ANCHOR_API_KEY || "";

export const options = {
  scenarios: {
    // A constant ARRIVAL RATE, not a fixed number of virtual users.
    //
    // With fixed VUs, a slowing server automatically slows the load -- the test
    // backs off exactly when the system is struggling, and the measurement
    // flatters it. A fixed arrival rate keeps pushing, which is what real
    // traffic does.
    steady: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 50),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 20,
      maxVUs: 200,
    },
  },
  thresholds: {
    // A 429 is not a failure: it is backpressure working. It is counted
    // separately so a saturated run is not mistaken for a broken one.
    "http_req_failed{expected_response:true}": ["rate<0.01"],
    anchor_submit_latency_ms: ["p(99)<1000"],
  },
};

export default function () {
  const res = http.post(
    `${baseUrl}/v1/runs`,
    JSON.stringify({ agent, input: { n: __ITER }, budget_tokens: 100000 }),
    {
      headers: {
        "content-type": "application/json",
        ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
      },
      tags: { name: "POST /v1/runs" },
    },
  );

  submitLatency.add(res.timings.duration);

  if (res.status === 429) {
    rejected.add(1);
    // Honour the server's own guidance rather than inventing an interval.
    sleep(Number(res.headers["Retry-After"] || 1));
    return;
  }

  check(res, { "submit accepted": (r) => r.status === 202 });
  if (res.status === 202) submitted.add(1);
}

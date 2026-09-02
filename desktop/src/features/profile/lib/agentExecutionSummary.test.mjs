import assert from "node:assert/strict";
import test from "node:test";

import { buildAgentExecutionFacts } from "./agentExecutionSummary.ts";

function agent(overrides = {}) {
  return {
    agentCommand: "buzz-agent",
    backend: { type: "local" },
    model: "gpt-5.6-luna",
    provider: "openai",
    ...overrides,
  };
}

test("reports local effective execution and optional model facts", () => {
  assert.deepEqual(buildAgentExecutionFacts(agent()), [
    { id: "location", label: "Runs on", value: "This computer" },
    {
      copyValue: "buzz-agent",
      id: "harness",
      label: "Harness",
      value: "buzz-agent",
    },
    {
      copyValue: "openai",
      id: "provider",
      label: "Model provider",
      value: "openai",
    },
    {
      copyValue: "gpt-5.6-luna",
      id: "model",
      label: "Model",
      value: "gpt-5.6-luna",
    },
  ]);
});

test("reports the selected remote backend without exposing its config", () => {
  const facts = buildAgentExecutionFacts(
    agent({
      backend: {
        type: "provider",
        id: "staging-compute",
        config: { token: "must-not-be-rendered" },
      },
      model: null,
      provider: null,
    }),
  );

  assert.deepEqual(facts, [
    {
      copyValue: "staging-compute",
      id: "location",
      label: "Runs on",
      value: "Remote server (staging-compute)",
    },
    {
      copyValue: "buzz-agent",
      id: "harness",
      label: "Harness",
      value: "buzz-agent",
    },
  ]);
  assert.equal(JSON.stringify(facts).includes("must-not-be-rendered"), false);
});

test("does not invent missing harness, provider, or model values", () => {
  assert.deepEqual(
    buildAgentExecutionFacts(
      agent({ agentCommand: "", model: " ", provider: null }),
    ),
    [{ id: "location", label: "Runs on", value: "This computer" }],
  );
});

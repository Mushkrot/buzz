import type { ManagedAgent } from "@/shared/api/types";

export type AgentExecutionFactId =
  | "location"
  | "harness"
  | "provider"
  | "model";

export type AgentExecutionFact = {
  copyValue?: string;
  id: AgentExecutionFactId;
  label: string;
  value: string;
};

/**
 * Build the effective execution facts reported by the managed-agent summary.
 * Optional values stay absent; the function never invents defaults for a
 * missing provider, model, or harness command.
 */
export function buildAgentExecutionFacts(
  agent: ManagedAgent,
): AgentExecutionFact[] {
  const facts: AgentExecutionFact[] = [
    agent.backend.type === "local"
      ? {
          id: "location",
          label: "Runs on",
          value: "This computer",
        }
      : {
          copyValue: agent.backend.id,
          id: "location",
          label: "Runs on",
          value: agent.backend.id.trim()
            ? `Remote server (${agent.backend.id})`
            : "Remote server",
        },
  ];
  const harness = agent.agentCommand.trim();
  const provider = agent.provider?.trim() ?? "";
  const model = agent.model?.trim() ?? "";

  if (harness) {
    facts.push({
      copyValue: harness,
      id: "harness",
      label: "Harness",
      value: harness,
    });
  }
  if (provider) {
    facts.push({
      copyValue: provider,
      id: "provider",
      label: "Model provider",
      value: provider,
    });
  }
  if (model) {
    facts.push({
      copyValue: model,
      id: "model",
      label: "Model",
      value: model,
    });
  }

  return facts;
}

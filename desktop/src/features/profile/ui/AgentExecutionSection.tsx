import {
  Cloud,
  Cpu,
  type LucideIcon,
  Monitor,
  Pencil,
  Server,
  Terminal,
} from "lucide-react";

import { formatAgentModelLabel } from "@/features/agents/lib/formatAgentModelLabel";
import { providerDisplayLabel } from "@/features/agents/ui/agentConfigOptions";
import {
  buildAgentExecutionFacts,
  type AgentExecutionFact,
  type AgentExecutionFactId,
} from "@/features/profile/lib/agentExecutionSummary";
import type { ManagedAgent } from "@/shared/api/types";
import {
  HoverCopyIndicator,
  useCopyFeedback,
} from "@/shared/ui/HoverCopyIndicator";
import { PanelSectionGroup } from "@/shared/ui/PanelSectionGroup";
import { Button } from "@/shared/ui/button";

type ExecutionRowProps = {
  fact: AgentExecutionFact;
  icon: LucideIcon;
  modelProvider?: string;
  testId: string;
};

function ExecutionRow({
  fact,
  icon: Icon,
  modelProvider,
  testId,
}: ExecutionRowProps) {
  const { copied, copy } = useCopyFeedback({
    label: fact.label,
    value: fact.copyValue ?? "",
  });

  const content = (
    <>
      <Icon
        className="h-4 w-4 shrink-0 text-muted-foreground"
        data-slot="profile-field-icon"
      />
      <span className="min-w-0 flex-1">
        <span className="block text-sm font-medium text-foreground">
          {fact.label}
        </span>
        <span
          className="mt-0.5 block truncate text-sm text-muted-foreground/70"
          title={fact.value}
        >
          {fact.id === "provider"
            ? providerDisplayLabel(fact.value)
            : fact.id === "model"
              ? formatAgentModelLabel(fact.value, modelProvider)
              : fact.value}
        </span>
      </span>
      {fact.copyValue ? (
        <HoverCopyIndicator copied={copied} testId={`${testId}-copy-status`} />
      ) : null}
    </>
  );

  if (fact.copyValue) {
    return (
      <button
        className="group flex min-h-16 w-full items-center gap-3 px-4 py-3 text-left transition-colors hover:bg-muted/40 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
        data-testid={testId}
        onClick={() => void copy()}
        type="button"
      >
        {content}
      </button>
    );
  }

  return (
    <div
      className="flex min-h-16 items-center gap-3 px-4 py-3"
      data-testid={testId}
    >
      {content}
    </div>
  );
}

const FACT_ICONS: Record<AgentExecutionFactId, LucideIcon> = {
  location: Monitor,
  harness: Terminal,
  provider: Server,
  model: Cpu,
};

export function AgentExecutionSection({
  agent,
  onEdit,
}: {
  agent: ManagedAgent;
  onEdit?: () => void;
}) {
  const facts = buildAgentExecutionFacts(agent);
  const modelProvider = facts.find((fact) => fact.id === "provider")?.value;

  return (
    <PanelSectionGroup
      headerAction={
        onEdit ? (
          <Button
            aria-label="Edit execution settings"
            data-testid="user-profile-execution-edit"
            onClick={onEdit}
            size="xs"
            title="Edit execution settings"
            type="button"
            variant="outline"
          >
            <Pencil />
            Edit
          </Button>
        ) : null
      }
      testId="user-profile-execution-section"
      title="Execution"
    >
      {facts.map((fact) => (
        <ExecutionRow
          fact={fact}
          icon={
            fact.id === "location" && agent.backend.type === "provider"
              ? Cloud
              : FACT_ICONS[fact.id]
          }
          key={fact.id}
          modelProvider={modelProvider}
          testId={`user-profile-execution-${fact.id}`}
        />
      ))}
    </PanelSectionGroup>
  );
}


export interface DeltaWithReasoning {
  content?: string | null;
  reasoning_content?: string | null;
  reasoning?: string | null;
  reasoning_details?: Array<{ text?: string | null }> | null;
}

const THINK_OPEN = "<think>";
const THINK_CLOSE = "</think>";

export function reasoningFromDelta(
  delta: DeltaWithReasoning | undefined,
): string {
  if (!delta) return "";
  if (delta.reasoning_content) return delta.reasoning_content;
  if (delta.reasoning) return delta.reasoning;
  if (delta.reasoning_details?.length) {
    return delta.reasoning_details.map((d) => d?.text ?? "").join("");
  }
  return "";
}

export function splitThinkTags(content: string): {
  display: string;
  thinking: string;
} {
  if (!content.includes(THINK_OPEN)) return { display: content, thinking: "" };
  let display = "";
  const thinking: string[] = [];
  let rest = content;
  for (;;) {
    const open = rest.indexOf(THINK_OPEN);
    if (open === -1) {
      display += rest;
      break;
    }
    display += rest.slice(0, open);
    const afterOpen = rest.slice(open + THINK_OPEN.length);
    const close = afterOpen.indexOf(THINK_CLOSE);
    if (close === -1) {
      thinking.push(afterOpen);
      break;
    }
    thinking.push(afterOpen.slice(0, close));
    rest = afterOpen.slice(close + THINK_CLOSE.length);
  }
  return { display, thinking: thinking.join("\n\n") };
}

export interface ReasoningAccumulator {
  rawContent: string;
  fieldReasoning: string;
}

export function createReasoningAccumulator(): ReasoningAccumulator {
  return { rawContent: "", fieldReasoning: "" };
}

export interface AppliedDelta {
  contentDelta: string;
  reasoningDelta: string;
  content: string;
  reasoning: string;
}

export function applyDelta(
  acc: ReasoningAccumulator,
  delta: DeltaWithReasoning | undefined,
): AppliedDelta {
  const contentDelta = delta?.content ?? "";
  const reasoningDelta = reasoningFromDelta(delta);
  acc.rawContent += contentDelta;
  acc.fieldReasoning += reasoningDelta;
  return { contentDelta, reasoningDelta, ...finalizeReasoning(acc) };
}

export function finalizeReasoning(acc: ReasoningAccumulator): {
  content: string;
  reasoning: string;
} {
  const { display, thinking } = splitThinkTags(acc.rawContent);
  const reasoning = [acc.fieldReasoning, thinking].filter(Boolean).join("\n\n");
  return { content: display, reasoning };
}

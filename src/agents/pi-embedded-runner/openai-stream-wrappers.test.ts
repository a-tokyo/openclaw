import type { StreamFn } from "@mariozechner/pi-agent-core";
import type { Model } from "@mariozechner/pi-ai";
import { createAssistantMessageEventStream } from "@mariozechner/pi-ai";
import { describe, expect, it } from "vitest";
import { createOpenAIThinkingLevelWrapper } from "./openai-stream-wrappers.js";

function createPayloadCapture() {
  const payloads: Array<Record<string, unknown>> = [];
  const baseStreamFn: StreamFn = (model, _context, options) => {
    const payload: Record<string, unknown> = { model: model.id };
    options?.onPayload?.(payload, model);
    payloads.push(structuredClone(payload));
    return createAssistantMessageEventStream();
  };
  return { baseStreamFn, payloads };
}

const codexModel = {
  api: "openai-codex-responses",
  provider: "openai-codex",
  id: "gpt-5.1-codex",
} as Model<"openai-codex-responses">;

const openaiModel = {
  api: "openai-responses",
  provider: "openai",
  id: "gpt-5.2",
} as Model<"openai-responses">;

describe("createOpenAIThinkingLevelWrapper", () => {
  it("injects reasoning.effort medium when thinkingLevel is medium", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "medium");
    void wrapped(codexModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "medium" });
  });

  it("injects reasoning.effort high when thinkingLevel is high", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "high");
    void wrapped(openaiModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "high" });
  });

  it("injects reasoning.effort none when thinkingLevel is off", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "off");
    void wrapped(codexModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "none" });
  });

  it("maps adaptive thinkingLevel to medium effort", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "adaptive");
    void wrapped(codexModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "medium" });
  });

  it("overrides existing reasoning.effort from upstream wrappers", () => {
    const baseStreamFn: StreamFn = (model, _context, options) => {
      const payload: Record<string, unknown> = {
        model: model.id,
        reasoning: { effort: "none" },
      };
      options?.onPayload?.(payload, model);
      return createAssistantMessageEventStream();
    };

    const payloads: Array<Record<string, unknown>> = [];
    const capture: StreamFn = (model, context, options) => {
      return baseStreamFn(model, context, {
        ...options,
        onPayload: (payload, m) => {
          options?.onPayload?.(payload, m);
          payloads.push(structuredClone(payload as Record<string, unknown>));
        },
      });
    };

    const wrapped = createOpenAIThinkingLevelWrapper(capture, "medium");
    void wrapped(codexModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "medium" });
  });

  it("returns underlying streamFn unchanged when thinkingLevel is undefined", () => {
    const { baseStreamFn } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, undefined);
    expect(wrapped).toBe(baseStreamFn);
  });

  it("preserves other reasoning properties when overriding effort", () => {
    const baseStreamFn: StreamFn = (model, _context, options) => {
      const payload: Record<string, unknown> = {
        model: model.id,
        reasoning: { effort: "none", summary: "auto" },
      };
      options?.onPayload?.(payload, model);
      return createAssistantMessageEventStream();
    };

    const payloads: Array<Record<string, unknown>> = [];
    const capture: StreamFn = (model, context, options) => {
      return baseStreamFn(model, context, {
        ...options,
        onPayload: (payload, m) => {
          options?.onPayload?.(payload, m);
          payloads.push(structuredClone(payload as Record<string, unknown>));
        },
      });
    };

    const wrapped = createOpenAIThinkingLevelWrapper(capture, "high");
    void wrapped(codexModel, { messages: [] }, {});

    expect(payloads[0]?.reasoning).toEqual({ effort: "high", summary: "auto" });
  });

  it("does not inject reasoning for completions API on proxy routes", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "medium");
    void wrapped(
      {
        api: "openai-completions",
        provider: "openai",
        id: "gpt-4o",
        baseUrl: "https://proxy.example.com/v1",
      } as Model<"openai-completions">,
      { messages: [] },
      {},
    );

    expect(payloads[0]?.reasoning).toBeUndefined();
  });

  it("does not inject reasoning for proxy routes with custom baseUrl", () => {
    const { baseStreamFn, payloads } = createPayloadCapture();
    const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, "medium");
    void wrapped(
      {
        api: "openai-responses",
        provider: "openai",
        id: "gpt-5.2",
        baseUrl: "https://proxy.example.com/v1",
      } as Model<"openai-responses">,
      { messages: [] },
      {},
    );

    expect(payloads[0]?.reasoning).toBeUndefined();
  });

  it("passes through all thinking levels correctly", () => {
    const levels = ["minimal", "low", "medium", "high", "xhigh"] as const;
    for (const level of levels) {
      const { baseStreamFn, payloads } = createPayloadCapture();
      const wrapped = createOpenAIThinkingLevelWrapper(baseStreamFn, level);
      void wrapped(codexModel, { messages: [] }, {});
      expect(payloads[0]?.reasoning).toEqual({ effort: level });
    }
  });
});

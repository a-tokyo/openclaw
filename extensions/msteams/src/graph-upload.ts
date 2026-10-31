import { bufferToBlobPart } from "openclaw/plugin-sdk/blob-runtime";
import { pruneMapToMaxSize } from "openclaw/plugin-sdk/collection-runtime";
import { responseWithRelease } from "openclaw/plugin-sdk/fetch-runtime";
import {
  createProviderHttpError,
  readProviderJsonResponse,
} from "openclaw/plugin-sdk/provider-http";
import { fetchWithSsrFGuard } from "openclaw/plugin-sdk/ssrf-runtime";
import type { MSTeamsAccessTokenProvider } from "./attachments/types.js";
import type { MSTeamsSdkCloudOptions } from "./cloud.js";
import type { StoredConversationReference } from "./conversation-store.js";
import {
  resolveMSTeamsSharePointUploadTimeoutMs,
  withMSTeamsAbortableRequestTimeout,
  withMSTeamsRequestDeadline,
} from "./request-timeout.js";
import { getMSTeamsTeamDetailsWithReference } from "./sdk-proactive.js";
import type { MSTeamsApp } from "./sdk.js";
import { assertMSTeamsSendHandoff, type MSTeamsSendHandoff } from "./send-handoff.js";
import { resolveTeamGroupId } from "./team-identity.js";
import { buildUserAgent } from "./user-agent.js";

const GRAPH_ROOT = "https://graph.microsoft.com/v1.0";
const GRAPH_BETA = "https://graph.microsoft.com/beta";
const GRAPH_SCOPE = "https://graph.microsoft.com";

// Sites are stable team metadata. Bound retained entries and refresh on process restart.
const teamSiteIds = new Map<string, string>();
const TEAM_SITE_CACHE_MAX_ENTRIES = 500;

export async function resolveMSTeamsSharePointSiteId(
  params: {
    siteId?: string;
    accountId?: string | null;
    app: MSTeamsApp;
    conversationRef: StoredConversationReference;
    tokenProvider: MSTeamsAccessTokenProvider;
    serviceUrlBoundary?: MSTeamsSdkCloudOptions;
  } & MSTeamsSendHandoff,
): Promise<string> {
  if (params.siteId !== undefined) {
    const siteId = params.siteId.trim();
    if (!siteId) {
      throw new Error("channels.msteams.sharePointSiteId must not be blank");
    }
    return siteId;
  }
  const ref = params.conversationRef;
  const conversation = ref.conversation;
  const conversationId = conversation?.id;
  if (
    conversation?.conversationType?.toLowerCase() !== "channel" ||
    !conversationId ||
    !ref.teamId
  ) {
    throw new Error(
      "channels.msteams.sharePointSiteId is required to send files to group chats or channels",
    );
  }
  assertMSTeamsSendHandoff(params);
  const teamId = await resolveTeamGroupId({
    conversationTeamId: ref.teamId,
    getTeamDetails: (conversationTeamId) =>
      getMSTeamsTeamDetailsWithReference(
        params.app,
        { ...ref, conversation: { ...conversation, id: conversationId } },
        conversationTeamId,
        params,
      ),
  });
  if (!teamId) {
    throw new Error("Unable to resolve the Teams group; set channels.msteams.sharePointSiteId");
  }
  const channelId = conversationId.split(";")[0] ?? conversationId;
  const channel = await requestSharePointJson<{ membershipType?: string }>(params, {
    url: `${GRAPH_ROOT}/teams/${encodeURIComponent(teamId)}/channels/${encodeURIComponent(channelId)}?$select=membershipType`,
    error: "Get Teams channel failed; verify the existing ChannelSettings.Read.Group RSC grant",
    label: "msteams.graph-upload.channel",
  });
  // Private/shared channels have a separate audience and must never use the parent team's site.
  if (channel.membershipType !== "standard") {
    throw new Error(
      "Automatic SharePoint site discovery supports standard channels only; set channels.msteams.sharePointSiteId",
    );
  }
  const cacheKey = JSON.stringify([
    params.accountId ?? "default",
    ref.tenantId ?? conversation.tenantId,
    (ref.agent ?? ref.bot)?.id,
    teamId,
  ]);
  const cached = teamSiteIds.get(cacheKey);
  if (cached) {
    return cached;
  }
  const site = await requestSharePointJson<{ id?: string }>(params, {
    url: `${GRAPH_ROOT}/groups/${encodeURIComponent(teamId)}/sites/root`,
    error: "Resolve Teams SharePoint site failed",
    label: "msteams.graph-upload.teamSite",
  });
  if (!site.id?.trim()) {
    throw new Error(
      "Teams SharePoint site response missing id; set channels.msteams.sharePointSiteId",
    );
  }
  teamSiteIds.set(cacheKey, site.id);
  pruneMapToMaxSize(teamSiteIds, TEAM_SITE_CACHE_MAX_ENTRIES);
  return site.id;
}

interface DriveUploadResult {
  id: string;
  webUrl: string;
  name: string;
}

const SHAREPOINT_REQUEST_TIMEOUT_LABEL = "MS Teams SharePoint request";
const SHAREPOINT_UPLOAD_TIMEOUT_LABEL = "MS Teams SharePoint upload";
const GRAPH_TOKEN_TIMEOUT_LABEL = "MS Teams Graph token acquisition";

async function requestSharePointJson<T>(
  params: {
    tokenProvider: MSTeamsAccessTokenProvider;
  } & MSTeamsSendHandoff,
  request: {
    url: string;
    init?: () => Pick<RequestInit, "method" | "body"> & { headers?: Record<string, string> };
    label: string;
    error: string | ((response: Response) => string);
    timeoutMs?: number;
  },
): Promise<T> {
  return await withMSTeamsAbortableRequestTimeout({
    label:
      request.timeoutMs !== undefined
        ? SHAREPOINT_UPLOAD_TIMEOUT_LABEL
        : SHAREPOINT_REQUEST_TIMEOUT_LABEL,
    timeoutMs: request.timeoutMs,
    work: async (signal) => {
      assertMSTeamsSendHandoff(params);
      const token = await withMSTeamsRequestDeadline({
        label: GRAPH_TOKEN_TIMEOUT_LABEL,
        work: () => params.tokenProvider.getAccessToken(GRAPH_SCOPE),
      });
      assertMSTeamsSendHandoff(params);
      const init = request.init?.();
      const { response, release } = await fetchWithSsrFGuard({
        url: request.url,
        init: {
          ...init,
          headers: {
            "User-Agent": buildUserAgent(),
            Authorization: `Bearer ${token}`,
            ...init?.headers,
          },
          signal,
        },
        mode: "trusted_env_proxy",
        beforeRequest: () => assertMSTeamsSendHandoff(params),
        // Preserve fetch's redirect limit, method/body replay, and cross-origin
        // credential stripping while checking authority again before each hop.
        maxRedirects: 20,
        allowCrossOriginUnsafeRedirectReplay: true,
        auditContext: "msteams.graph-upload",
      });
      const res = responseWithRelease(response, release);
      if (!res.ok) {
        throw await createProviderHttpError(
          res,
          typeof request.error === "string" ? request.error : request.error(res),
        );
      }
      return await readProviderJsonResponse<T>(res, request.label, {
        chunkTimeoutMs: request.timeoutMs,
      });
    },
  });
}

async function uploadToSharePoint(
  params: {
    buffer: Buffer;
    filename: string;
    contentType?: string;
    tokenProvider: MSTeamsAccessTokenProvider;
    siteId: string;
  } & MSTeamsSendHandoff,
): Promise<DriveUploadResult> {
  const uploadPath = `/OpenClawShared/${encodeURIComponent(params.filename)}`;
  // Graph's default conflictBehavior=replace overwrites a same-named file in place. Bot assets
  // reuse names (image-1.png each generation) and Teams caches file cards by driveItem URL, so
  // replace clobbers history and shows stale images; "rename" mints a unique driveItem instead.
  const uploadUrl = `${GRAPH_ROOT}/sites/${params.siteId}/drive/root:${uploadPath}:/content?@microsoft.graph.conflictBehavior=rename`;
  const timeoutMs = resolveMSTeamsSharePointUploadTimeoutMs(params.buffer.length);

  const data = await requestSharePointJson<Partial<DriveUploadResult>>(params, {
    url: uploadUrl,
    timeoutMs,
    init: () => ({
      method: "PUT",
      headers: { "Content-Type": params.contentType ?? "application/octet-stream" },
      body: new Blob([bufferToBlobPart(params.buffer)]),
    }),
    error: "SharePoint upload failed",
    label: "msteams.graph-upload.uploadSharePointFile",
  });

  if (!data.id || !data.webUrl || !data.name) {
    throw new Error("SharePoint upload response missing required fields");
  }

  return {
    id: data.id,
    webUrl: data.webUrl,
    name: data.name,
  };
}

/**
 * Properties needed for native Teams file card attachments.
 * The eTag is used as the attachment ID and webDavUrl as the contentUrl.
 */
export interface DriveItemProperties {
  eTag: string;
  webDavUrl: string;
  name: string;
}

export async function getDriveItemProperties(
  params: {
    siteId: string;
    itemId: string;
    tokenProvider: MSTeamsAccessTokenProvider;
  } & MSTeamsSendHandoff,
): Promise<DriveItemProperties> {
  const data = await requestSharePointJson<Partial<DriveItemProperties>>(params, {
    url: `${GRAPH_ROOT}/sites/${params.siteId}/drive/items/${params.itemId}?$select=eTag,webDavUrl,name`,
    error: "Get driveItem properties failed",
    label: "msteams.graph-upload.getDriveItemProperties",
  });

  if (!data.eTag || !data.webDavUrl || !data.name) {
    throw new Error("DriveItem response missing required properties (eTag, webDavUrl, or name)");
  }

  return {
    eTag: data.eTag,
    webDavUrl: data.webDavUrl,
    name: data.name,
  };
}

async function getChatMemberIds(
  params: {
    chatId: string;
    tokenProvider: MSTeamsAccessTokenProvider;
  } & MSTeamsSendHandoff,
): Promise<string[]> {
  const data = await requestSharePointJson<{ value?: Array<{ userId?: string }> }>(params, {
    url: `${GRAPH_ROOT}/chats/${params.chatId}/members`,
    // Graph 403 covers permissions, licensing, and conditional access. RSC
    // grants are not token roles, so no local signal can safely widen access.
    error: (res) =>
      res.status === 403
        ? "Get chat members failed; verify Graph chat-member permissions and tenant access policies"
        : "Get chat members failed",
    label: "msteams.graph-upload.getChatMembers",
  });
  return (data.value ?? []).flatMap((member) => (member.userId ? [member.userId] : []));
}

/**
 * Create a sharing link for a SharePoint drive item.
 * For organization scope (default), uses v1.0 API.
 * For per-user scope, uses beta API with recipients.
 */
async function createSharePointSharingLink(
  params: {
    siteId: string;
    itemId: string;
    tokenProvider: MSTeamsAccessTokenProvider;
    /** AAD object IDs for a per-user link; omitted for an organization link. */
    recipientObjectIds?: string[];
  } & MSTeamsSendHandoff,
): Promise<string> {
  const apiRoot = params.recipientObjectIds ? GRAPH_BETA : GRAPH_ROOT;

  const body: Record<string, unknown> = {
    type: "view",
    scope: params.recipientObjectIds ? "users" : "organization",
  };

  if (params.recipientObjectIds) {
    body.recipients = params.recipientObjectIds.map((id) => ({ objectId: id }));
  }

  const data = await requestSharePointJson<{ link?: { webUrl?: string } }>(params, {
    url: `${apiRoot}/sites/${params.siteId}/drive/items/${params.itemId}/createLink`,
    init: () => ({
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    error: "Create SharePoint sharing link failed",
    label: "msteams.graph-upload.createSharePointSharingLink",
  });

  if (!data.link?.webUrl) {
    throw new Error("Create SharePoint sharing link response missing webUrl");
  }

  return data.link.webUrl;
}

/**
 * Upload a file to SharePoint and create a sharing link.
 *
 * For group chats, this creates a per-user sharing link scoped to chat members.
 * For channels, this creates an organization-wide sharing link.
 */
export async function uploadAndShareSharePoint(
  params: {
    buffer: Buffer;
    filename: string;
    contentType?: string;
    tokenProvider: MSTeamsAccessTokenProvider;
    siteId: string;
    chatId?: string;
    usePerUserSharing?: boolean;
  } & MSTeamsSendHandoff,
) {
  const uploaded = await uploadToSharePoint(params);

  let recipientObjectIds: string[] | undefined;

  if (params.usePerUserSharing && params.chatId) {
    recipientObjectIds = await getChatMemberIds({
      chatId: params.chatId,
      tokenProvider: params.tokenProvider,
      assertDirectAdapterHandoff: params.assertDirectAdapterHandoff,
    });
    if (recipientObjectIds.length === 0) {
      throw new Error("MS Teams chat member lookup returned no recipients");
    }
  }

  const shareUrl = await createSharePointSharingLink({
    siteId: params.siteId,
    itemId: uploaded.id,
    tokenProvider: params.tokenProvider,
    recipientObjectIds,
    assertDirectAdapterHandoff: params.assertDirectAdapterHandoff,
  });

  return {
    itemId: uploaded.id,
    webUrl: uploaded.webUrl,
    shareUrl,
    name: uploaded.name,
  };
}

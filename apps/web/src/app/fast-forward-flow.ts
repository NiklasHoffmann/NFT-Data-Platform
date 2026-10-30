import { fastForwardCollectionIndexing } from "@nft-platform/db";
import { evmAddressSchema, normalizeContractAddress } from "@nft-platform/domain";
import { z } from "zod";
import { getWebMongoDatabase } from "../lib/mongodb";

const fastForwardFormSchema = z.object({
  chainId: z.coerce.number().int().positive(),
  contractAddress: evmAddressSchema
});

export type FastForwardOutcome = {
  status: "done" | "invalid" | "failed";
  message: string;
};

/**
 * Moves one collection's indexer checkpoint up to the head it has already seen.
 *
 * This abandons the transfers in the skipped range rather than reading them, which is
 * the point: a checkpoint far enough behind the head turns every range query into an
 * archive request, and a public RPC refuses those. Running this trades that history
 * for an indexer that makes progress again. The gap can be read later by pointing
 * RPC_URL_<chainId> at a provider with archive access and reindexing the range.
 */
export async function handleFastForwardSubmission(formData: FormData): Promise<FastForwardOutcome> {
  const parsed = fastForwardFormSchema.safeParse({
    chainId: formData.get("chainId") ?? undefined,
    contractAddress: formData.get("contractAddress") ?? undefined
  });

  if (!parsed.success) {
    return {
      status: "invalid",
      message: parsed.error.issues[0]?.message ?? "Invalid fast-forward request."
    };
  }

  const contractAddress = normalizeContractAddress(parsed.data.contractAddress);

  try {
    const result = await fastForwardCollectionIndexing({
      database: getWebMongoDatabase(),
      chainId: parsed.data.chainId,
      contractAddress
    });

    if (result.outcome === "unknown-collection") {
      return {
        status: "invalid",
        message: `No collection is indexed for ${contractAddress} on chain ${parsed.data.chainId}.`
      };
    }

    if (result.outcome === "already-current") {
      return {
        status: "done",
        message: `${contractAddress} is already at the head it has observed. Nothing to skip.`
      };
    }

    return {
      status: "done",
      message: `${contractAddress}: checkpoint moved to block ${result.skippedTo}. ${result.skippedBlocks} blocks from ${result.skippedFrom ?? "the deploy block"} onwards were skipped and will not be read.`
    };
  } catch {
    return {
      status: "failed",
      message: "Could not move the checkpoint. Check that MongoDB is reachable."
    };
  }
}

export function buildFastForwardRedirectUrl(outcome: FastForwardOutcome): string {
  const searchParams = new URLSearchParams({
    view: "operations",
    status: outcome.status === "done" ? "loaded" : "failed",
    message: outcome.message
  });

  return `/?${searchParams.toString()}`;
}

import process from "node:process";
import {
  getChainPublicClient,
  getRpcUrlForChain,
  hasContractBytecode,
  isSupportedChainId
} from "@nft-platform/chain";
import { normalizeContractAddress, supportedChains } from "@nft-platform/domain";

const zeroAddress = "0x0000000000000000000000000000000000000000";

type ContractAddressGuardResult =
  | { ok: true }
  | { ok: false; message: string };

export async function guardContractAddress(params: {
  chainId: number;
  contractAddress: string;
}): Promise<ContractAddressGuardResult> {
  if (!isSupportedChainId(params.chainId)) {
    return {
      ok: false,
      message: `Unsupported chainId ${params.chainId}.`
    };
  }

  const normalizedContractAddress = normalizeContractAddress(params.contractAddress);

  if (normalizedContractAddress === zeroAddress) {
    return {
      ok: false,
      message: "The zero address cannot be used as a contract address."
    };
  }

  const rpcUrls = buildRpcUrlsFromEnv();
  const rpcUrl = rpcUrls[params.chainId];

  if (!rpcUrl) {
    // If no RPC is configured for the web service, we can only validate syntax-level constraints.
    return { ok: true };
  }

  try {
    const client = getChainPublicClient({
      chainId: params.chainId,
      rpcUrl: getRpcUrlForChain({
        chainId: params.chainId,
        rpcUrls
      })
    });
    const hasBytecode = await hasContractBytecode({
      client,
      contractAddress: normalizedContractAddress
    });

    if (!hasBytecode) {
      return {
        ok: false,
        message: "Address has no deployed contract bytecode on the selected chain."
      };
    }

    return { ok: true };
  } catch (error) {
    console.warn("[web] contract pre-check skipped due to RPC error", {
      chainId: params.chainId,
      contractAddress: normalizedContractAddress,
      error
    });

    // Do not block requests when RPC temporarily fails; worker-level validation still runs.
    return { ok: true };
  }
}

function buildRpcUrlsFromEnv(): Record<number, string> {
  const rpcUrls: Record<number, string> = {};

  for (const chain of supportedChains) {
    const url = process.env[`RPC_URL_${chain.id}`];

    if (url?.trim()) {
      rpcUrls[chain.id] = url.trim();
    }
  }

  if (process.env.RPC_MAINNET_URL?.trim() && !rpcUrls[1]) {
    rpcUrls[1] = process.env.RPC_MAINNET_URL.trim();
  }

  if (process.env.RPC_SEPOLIA_URL?.trim() && !rpcUrls[11155111]) {
    rpcUrls[11155111] = process.env.RPC_SEPOLIA_URL.trim();
  }

  return rpcUrls;
}

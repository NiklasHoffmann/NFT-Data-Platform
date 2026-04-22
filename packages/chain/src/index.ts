import {
  normalizeContractAddress,
  normalizeWalletAddress,
  supportedChainIds,
  type NftStandard
} from "@nft-platform/domain";
import {
  getAddress,
  createPublicClient,
  http,
  parseAbi,
  parseAbiItem,
  toHex,
  type Address,
  type PublicClient
} from "viem";
import { mainnet, sepolia } from "viem/chains";

const erc165Abi = parseAbi([
  "function supportsInterface(bytes4 interfaceId) view returns (bool)"
]);

const erc721MetadataAbi = parseAbi([
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function tokenURI(uint256 tokenId) view returns (string)",
  "function ownerOf(uint256 tokenId) view returns (address)"
]);

const collectionMetadataAbi = parseAbi([
  "function name() view returns (string)",
  "function symbol() view returns (string)",
  "function contractURI() view returns (string)"
]);

const ownableAbi = parseAbi(["function owner() view returns (address)"]);

const erc2981Abi = parseAbi([
  "function royaltyInfo(uint256 tokenId, uint256 salePrice) view returns (address receiver, uint256 royaltyAmount)"
]);

const erc721SupplyAbi = parseAbi([
  "function totalSupply() view returns (uint256)"
]);

const erc1155MetadataAbi = parseAbi([
  "function uri(uint256 id) view returns (string)"
]);

const erc1155SupplyAbi = parseAbi([
  "function totalSupply(uint256 id) view returns (uint256)"
]);

const erc1155ExistsAbi = parseAbi([
  "function exists(uint256 id) view returns (bool)"
]);

const erc1155TransferSingleEvent = parseAbiItem(
  "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)"
);

const erc1155TransferBatchEvent = parseAbiItem(
  "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)"
);

const erc721TransferEvent = parseAbiItem(
  "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)"
);

const erc721TransferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

const zeroWalletAddress = normalizeWalletAddress("0x0000000000000000000000000000000000000000");

const supportedChainsById = {
  1: mainnet,
  11155111: sepolia
} as const;

const globalChainRegistry = globalThis as typeof globalThis & {
  __nftPlatformPublicClients__?: Map<string, PublicClient>;
};

export function isSupportedChainId(chainId: number): boolean {
  return supportedChainIds.includes(chainId as (typeof supportedChainIds)[number]);
}

export function expandErc1155UriTemplate(template: string, tokenId: string): string {
  const paddedTokenId = BigInt(tokenId).toString(16).padStart(64, "0");
  return template.replace(/\{id\}/gi, paddedTokenId);
}

export function normalizeAssetUri(uri: string): string {
  if (uri.startsWith("ipfs://")) {
    const remainder = uri.slice("ipfs://".length).replace(/^\/+/, "");

    if (!remainder) {
      return uri;
    }

    if (remainder.startsWith("ipfs/")) {
      return `https://ipfs.io/${remainder}`;
    }

    if (remainder.startsWith("ipns/")) {
      return `https://dweb.link/${remainder}`;
    }

    const [namespace = "", ...pathSegments] = remainder.split("/");
    const normalizedPath = pathSegments.join("/");
    const route = looksLikeIpfsCid(namespace) ? "ipfs" : "ipns";

    return `https://dweb.link/${route}/${namespace}${normalizedPath ? `/${normalizedPath}` : ""}`;
  }

  if (uri.startsWith("ar://")) {
    return uri.replace("ar://", "https://arweave.net/");
  }

  return uri;
}

function looksLikeIpfsCid(value: string): boolean {
  return /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(value) || /^b[a-z2-7]{20,}$/i.test(value);
}

export function createChainPublicClient(params: {
  chainId: number;
  rpcUrl: string;
}): PublicClient {
  const chain = supportedChainsById[params.chainId as keyof typeof supportedChainsById];

  if (!chain) {
    throw new Error(`Unsupported chainId ${params.chainId}.`);
  }

  return createPublicClient({
    chain,
    transport: http(params.rpcUrl, {
      timeout: 10_000,
      retryCount: 2,
      retryDelay: 250
    })
  });
}

export function getChainPublicClient(params: {
  chainId: number;
  rpcUrl: string;
}): PublicClient {
  const registry = (globalChainRegistry.__nftPlatformPublicClients__ ??= new Map());
  const cacheKey = `${params.chainId}:${params.rpcUrl}`;
  const existingClient = registry.get(cacheKey);

  if (existingClient) {
    return existingClient;
  }

  const client = createChainPublicClient(params);
  registry.set(cacheKey, client);
  return client;
}

export async function detectNftStandard(params: {
  client: PublicClient;
  contractAddress: string;
  tokenIdHint?: string;
}): Promise<NftStandard | null> {
  const address = getAddress(params.contractAddress);
  const supportsErc721 = await safeReadContract<boolean>({
    client: params.client,
    address,
    abi: erc165Abi,
    functionName: "supportsInterface",
    args: ["0x80ac58cd"]
  });

  if (supportsErc721) {
    return "erc721";
  }

  const supportsErc1155 = await safeReadContract<boolean>({
    client: params.client,
    address,
    abi: erc165Abi,
    functionName: "supportsInterface",
    args: ["0xd9b67a26"]
  });

  if (supportsErc1155) {
    return "erc1155";
  }

  if (params.tokenIdHint) {
    const tokenId = BigInt(params.tokenIdHint);
    const [erc721TokenUri, erc721Owner, erc1155Uri] = await Promise.all([
      safeReadContract<string>({
        client: params.client,
        address,
        abi: erc721MetadataAbi,
        functionName: "tokenURI",
        args: [tokenId]
      }),
      safeReadContract<Address>({
        client: params.client,
        address,
        abi: erc721MetadataAbi,
        functionName: "ownerOf",
        args: [tokenId]
      }),
      safeReadContract<string>({
        client: params.client,
        address,
        abi: erc1155MetadataAbi,
        functionName: "uri",
        args: [tokenId]
      })
    ]);

    if (sanitizeOptionalString(erc721TokenUri) || erc721Owner) {
      return "erc721";
    }

    if (sanitizeOptionalString(erc1155Uri)) {
      return "erc1155";
    }
  }

  return null;
}

export async function readCollectionOnChain(params: {
  client: PublicClient;
  contractAddress: string;
  standard: NftStandard;
}): Promise<{
  name: string | null;
  symbol: string | null;
  contractUriRaw: string | null;
  contractUriResolved: string | null;
  totalSupply: string | null;
  latestBlock: number | null;
}> {
  const address = getAddress(params.contractAddress);
  const [name, symbol, contractUri, totalSupply, blockNumber] = await Promise.all([
    safeReadContract<string>({
      client: params.client,
      address,
      abi: collectionMetadataAbi,
      functionName: "name"
    }),
    safeReadContract<string>({
      client: params.client,
      address,
      abi: collectionMetadataAbi,
      functionName: "symbol"
    }),
    safeReadContract<string>({
      client: params.client,
      address,
      abi: collectionMetadataAbi,
      functionName: "contractURI"
    }),
    params.standard === "erc721"
      ? safeReadContract<bigint>({
          client: params.client,
          address,
          abi: erc721SupplyAbi,
          functionName: "totalSupply"
        })
      : Promise.resolve(null),
    params.client.getBlockNumber().then((value) => Number(value)).catch(() => null)
  ]);

  const sanitizedContractUri = sanitizeOptionalString(contractUri);

  return {
    name: sanitizeOptionalString(name),
    symbol: sanitizeOptionalString(symbol),
    contractUriRaw: sanitizedContractUri,
    contractUriResolved: sanitizedContractUri ? normalizeAssetUri(sanitizedContractUri) : null,
    totalSupply: totalSupply !== null ? totalSupply.toString() : null,
    latestBlock: blockNumber
  };
}

export async function readCollectionSignalsOnChain(params: {
  client: PublicClient;
  contractAddress: string;
  royaltyTokenIdHint?: string | null;
}): Promise<{
  contractOwnerAddress: string | null;
  royaltyRecipientAddress: string | null;
  royaltyBasisPoints: number | null;
}> {
  const address = getAddress(params.contractAddress);
  const [contractOwner, supportsErc2981] = await Promise.all([
    safeReadContract<Address>({
      client: params.client,
      address,
      abi: ownableAbi,
      functionName: "owner"
    }),
    safeReadContract<boolean>({
      client: params.client,
      address,
      abi: erc165Abi,
      functionName: "supportsInterface",
      args: ["0x2a55205a"]
    })
  ]);

  let royaltyRecipientAddress: string | null = null;
  let royaltyBasisPoints: number | null = null;

  if (supportsErc2981) {
    const royaltyInfo = await safeReadContract<readonly [Address, bigint]>({
      client: params.client,
      address,
      abi: erc2981Abi,
      functionName: "royaltyInfo",
      args: [BigInt(params.royaltyTokenIdHint ?? "0"), 10_000n]
    });

    if (royaltyInfo) {
      royaltyRecipientAddress = normalizeWalletAddress(royaltyInfo[0]);
      royaltyBasisPoints = Number(royaltyInfo[1]);
    }
  }

  return {
    contractOwnerAddress: contractOwner ? normalizeWalletAddress(contractOwner) : null,
    royaltyRecipientAddress,
    royaltyBasisPoints
  };
}

export async function findContractDeploymentBlock(params: {
  client: PublicClient;
  contractAddress: string;
  latestBlock?: number | null;
}): Promise<number | null> {
  const address = getAddress(params.contractAddress);
  const latestBlock = params.latestBlock ?? Number(await params.client.getBlockNumber());
  const latestBytecode = await params.client.getBytecode({
    address,
    blockNumber: BigInt(latestBlock)
  }).catch(() => null);

  if (!latestBytecode || latestBytecode === "0x") {
    return null;
  }

  let low = 0n;
  let high = BigInt(latestBlock);
  let found = high;

  while (low <= high) {
    const mid = (low + high) / 2n;
    const bytecode = await params.client.getBytecode({
      address,
      blockNumber: mid
    }).catch(() => null);

    if (bytecode && bytecode !== "0x") {
      found = mid;

      if (mid === 0n) {
        break;
      }

      high = mid - 1n;
    } else {
      low = mid + 1n;
    }
  }

  return Number(found);
}

export async function hasContractBytecode(params: {
  client: PublicClient;
  contractAddress: string;
}): Promise<boolean> {
  const address = getAddress(params.contractAddress);
  const bytecode = await params.client.getBytecode({ address }).catch(() => null);
  return Boolean(bytecode && bytecode !== "0x");
}

export async function readTokenOnChain(params: {
  client: PublicClient;
  contractAddress: string;
  standard: NftStandard;
  tokenId: string;
}): Promise<{
  metadataUriRaw: string | null;
  metadataUriResolved: string | null;
  ownerAddress: string | null;
  supplyQuantity: string | null;
}> {
  const address = getAddress(params.contractAddress);
  const tokenIdBigInt = BigInt(params.tokenId);

  if (params.standard === "erc721") {
    const [rawTokenUri, ownerAddress] = await Promise.all([
      safeReadContract<string>({
        client: params.client,
        address,
        abi: erc721MetadataAbi,
        functionName: "tokenURI",
        args: [tokenIdBigInt]
      }),
      safeReadContract<Address>({
        client: params.client,
        address,
        abi: erc721MetadataAbi,
        functionName: "ownerOf",
        args: [tokenIdBigInt]
      })
    ]);

    return {
      metadataUriRaw: sanitizeOptionalString(rawTokenUri),
      metadataUriResolved: rawTokenUri ? normalizeAssetUri(rawTokenUri) : null,
      ownerAddress: ownerAddress ? normalizeWalletAddress(ownerAddress) : null,
      supplyQuantity: null
    };
  }

  const [rawTokenUri, totalSupply] = await Promise.all([
    safeReadContract<string>({
      client: params.client,
      address,
      abi: erc1155MetadataAbi,
      functionName: "uri",
      args: [tokenIdBigInt]
    }),
    safeReadContract<bigint>({
      client: params.client,
      address,
      abi: erc1155SupplyAbi,
      functionName: "totalSupply",
      args: [tokenIdBigInt]
    })
  ]);

  return {
    metadataUriRaw: sanitizeOptionalString(rawTokenUri),
    metadataUriResolved: rawTokenUri
      ? normalizeAssetUri(expandErc1155UriTemplate(rawTokenUri, params.tokenId))
      : null,
    ownerAddress: null,
    supplyQuantity: totalSupply !== null ? totalSupply.toString() : null
  };
}

export async function readErc1155TokenExists(params: {
  client: PublicClient;
  contractAddress: string;
  tokenId: string;
}): Promise<boolean | null> {
  const address = getAddress(params.contractAddress);
  const tokenIdBigInt = BigInt(params.tokenId);

  return safeReadContract<boolean>({
    client: params.client,
    address,
    abi: erc1155ExistsAbi,
    functionName: "exists",
    args: [tokenIdBigInt]
  });
}

export async function readErc1155TransfersInRange(params: {
  client: PublicClient;
  contractAddress: string;
  fromBlock: number;
  toBlock: number;
  maxBlockRange?: number;
}): Promise<Array<{
  fromAddress: string | null;
  toAddress: string | null;
  tokenId: string;
  value: string;
  blockNumber: number;
  logIndex: number;
}>> {
  if (params.fromBlock > params.toBlock) {
    return [];
  }

  const address = getAddress(params.contractAddress);
  const chunkSize = BigInt(params.maxBlockRange ?? 2_000);
  const startBlock = BigInt(params.fromBlock);
  const endBlock = BigInt(params.toBlock);
  const transfers: Array<{
    fromAddress: string | null;
    toAddress: string | null;
    tokenId: string;
    value: string;
    blockNumber: number;
    logIndex: number;
  }> = [];

  for (let chunkStart = startBlock; chunkStart <= endBlock; chunkStart += chunkSize) {
    const chunkEnd = chunkStart + chunkSize - 1n > endBlock ? endBlock : chunkStart + chunkSize - 1n;
    const { singleLogs, batchLogs } = await readErc1155LogsForWindowWithFallback({
      client: params.client,
      address,
      fromBlock: chunkStart,
      toBlock: chunkEnd
    });

    for (const log of singleLogs) {
      const args = log.args as {
        from?: Address;
        to?: Address;
        id?: bigint;
        value?: bigint;
      };

      if (args.id === undefined || args.value === undefined) {
        continue;
      }

      transfers.push({
        fromAddress: normalizeTransferAddress(args.from),
        toAddress: normalizeTransferAddress(args.to),
        tokenId: args.id.toString(),
        value: args.value.toString(),
        blockNumber: Number(log.blockNumber ?? 0n),
        logIndex: Number(log.logIndex ?? 0)
      });
    }

    for (const log of batchLogs) {
      const args = log.args as {
        from?: Address;
        to?: Address;
        ids?: bigint[];
        values?: bigint[];
      };

      if (!args.ids || !args.values || args.ids.length !== args.values.length) {
        continue;
      }

      for (let index = 0; index < args.ids.length; index += 1) {
        const tokenId = args.ids[index];
        const value = args.values[index];

        if (tokenId === undefined || value === undefined) {
          continue;
        }

        transfers.push({
          fromAddress: normalizeTransferAddress(args.from),
          toAddress: normalizeTransferAddress(args.to),
          tokenId: tokenId.toString(),
          value: value.toString(),
          blockNumber: Number(log.blockNumber ?? 0n),
          logIndex: Number(log.logIndex ?? 0)
        });
      }
    }
  }

  return transfers.sort((left, right) => {
    if (left.blockNumber !== right.blockNumber) {
      return left.blockNumber - right.blockNumber;
    }

    return left.logIndex - right.logIndex;
  });
}

export async function readErc721TransfersInRange(params: {
  client: PublicClient;
  contractAddress: string;
  fromBlock: number;
  toBlock: number;
  maxBlockRange?: number;
}): Promise<Array<{
  fromAddress: string | null;
  toAddress: string | null;
  tokenId: string;
  blockNumber: number;
  logIndex: number;
}>> {
  if (params.fromBlock > params.toBlock) {
    return [];
  }

  const address = getAddress(params.contractAddress);
  const chunkSize = BigInt(params.maxBlockRange ?? 1_000);
  const startBlock = BigInt(params.fromBlock);
  const endBlock = BigInt(params.toBlock);
  const transfers: Array<{
    fromAddress: string | null;
    toAddress: string | null;
    tokenId: string;
    blockNumber: number;
    logIndex: number;
  }> = [];

  for (let chunkStart = startBlock; chunkStart <= endBlock; chunkStart += chunkSize) {
    const chunkEnd = chunkStart + chunkSize - 1n > endBlock ? endBlock : chunkStart + chunkSize - 1n;
    const logs = (await params.client.request({
      method: "eth_getLogs",
      params: [
        {
          address,
          fromBlock: toHex(chunkStart),
          toBlock: toHex(chunkEnd),
          topics: [erc721TransferEventTopic]
        }
      ]
    })) as Array<{
      topics?: string[];
      blockNumber?: string;
      logIndex?: string;
    }>;

    for (const log of logs) {
      const [, fromTopic, toTopic, tokenIdTopic] = log.topics ?? [];

      if (!fromTopic || !toTopic || !tokenIdTopic) {
        continue;
      }

      transfers.push({
        fromAddress: normalizeTransferAddress(topicToAddress(fromTopic)),
        toAddress: normalizeTransferAddress(topicToAddress(toTopic)),
        tokenId: BigInt(tokenIdTopic).toString(),
        blockNumber: Number(BigInt(log.blockNumber ?? "0x0")),
        logIndex: Number(BigInt(log.logIndex ?? "0x0"))
      });
    }
  }

  return transfers.sort((left, right) => {
    if (left.blockNumber !== right.blockNumber) {
      return left.blockNumber - right.blockNumber;
    }

    return left.logIndex - right.logIndex;
  });
}

export async function hasErc1155TokenTransferActivity(params: {
  client: PublicClient;
  contractAddress: string;
  tokenId: string;
  fromBlock: number;
  toBlock: number;
  maxBlockRange?: number;
}): Promise<boolean | null> {
  if (params.fromBlock > params.toBlock) {
    return false;
  }

  const address = getAddress(params.contractAddress);
  const targetTokenId = BigInt(params.tokenId);
  const chunkSize = BigInt(params.maxBlockRange ?? 2_000);
  const startBlock = BigInt(params.fromBlock);
  const endBlock = BigInt(params.toBlock);

  try {
    for (let chunkStart = startBlock; chunkStart <= endBlock; chunkStart += chunkSize) {
      const chunkEnd = chunkStart + chunkSize - 1n > endBlock ? endBlock : chunkStart + chunkSize - 1n;
      const { singleLogs, batchLogs } = await readErc1155LogsForWindowWithFallback({
        client: params.client,
        address,
        fromBlock: chunkStart,
        toBlock: chunkEnd
      });

      for (const log of singleLogs) {
        const args = log.args as {
          id?: bigint;
        };

        if (args.id === targetTokenId) {
          return true;
        }
      }

      for (const log of batchLogs) {
        const args = log.args as {
          ids?: bigint[];
        };

        if (args.ids?.some((tokenId) => tokenId === targetTokenId)) {
          return true;
        }
      }
    }
  } catch {
    return null;
  }

  return false;
}

export async function readErc1155TransfersForTokenInRange(params: {
  client: PublicClient;
  contractAddress: string;
  tokenId: string;
  fromBlock: number;
  toBlock: number;
  maxBlockRange?: number;
}): Promise<Array<{
  fromAddress: string | null;
  toAddress: string | null;
  tokenId: string;
  value: string;
  blockNumber: number;
  logIndex: number;
}>> {
  if (params.fromBlock > params.toBlock) {
    return [];
  }

  const address = getAddress(params.contractAddress);
  const targetTokenId = BigInt(params.tokenId);
  const chunkSize = BigInt(params.maxBlockRange ?? 2_000);
  const startBlock = BigInt(params.fromBlock);
  const endBlock = BigInt(params.toBlock);
  const transfers: Array<{
    fromAddress: string | null;
    toAddress: string | null;
    tokenId: string;
    value: string;
    blockNumber: number;
    logIndex: number;
  }> = [];

  for (let chunkStart = startBlock; chunkStart <= endBlock; chunkStart += chunkSize) {
    const chunkEnd = chunkStart + chunkSize - 1n > endBlock ? endBlock : chunkStart + chunkSize - 1n;
    const { singleLogs, batchLogs } = await readErc1155LogsForWindowWithFallback({
      client: params.client,
      address,
      fromBlock: chunkStart,
      toBlock: chunkEnd
    });

    for (const log of singleLogs) {
      const args = log.args as {
        from?: Address;
        to?: Address;
        id?: bigint;
        value?: bigint;
      };

      if (args.id !== targetTokenId || args.value === undefined) {
        continue;
      }

      transfers.push({
        fromAddress: normalizeTransferAddress(args.from),
        toAddress: normalizeTransferAddress(args.to),
        tokenId: params.tokenId,
        value: args.value.toString(),
        blockNumber: Number(log.blockNumber ?? 0n),
        logIndex: Number(log.logIndex ?? 0)
      });
    }

    for (const log of batchLogs) {
      const args = log.args as {
        from?: Address;
        to?: Address;
        ids?: bigint[];
        values?: bigint[];
      };

      if (!args.ids || !args.values || args.ids.length !== args.values.length) {
        continue;
      }

      for (let index = 0; index < args.ids.length; index += 1) {
        const tokenId = args.ids[index];
        const value = args.values[index];

        if (tokenId !== targetTokenId || value === undefined) {
          continue;
        }

        transfers.push({
          fromAddress: normalizeTransferAddress(args.from),
          toAddress: normalizeTransferAddress(args.to),
          tokenId: params.tokenId,
          value: value.toString(),
          blockNumber: Number(log.blockNumber ?? 0n),
          logIndex: Number(log.logIndex ?? 0)
        });
      }
    }
  }

  return transfers.sort((left, right) => {
    if (left.blockNumber !== right.blockNumber) {
      return left.blockNumber - right.blockNumber;
    }

    return left.logIndex - right.logIndex;
  });
}

export function getRpcUrlForChain(params: {
  chainId: number;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
}): string {
  switch (params.chainId) {
    case 1:
      return params.rpcMainnetUrl;
    case 11155111:
      return params.rpcSepoliaUrl;
    default:
      throw new Error(`Unsupported chainId ${params.chainId}.`);
  }
}

async function safeReadContract<T>(params: {
  client: PublicClient;
  address: Address;
  abi: ReturnType<typeof parseAbi>;
  functionName: string;
  args?: readonly unknown[];
}): Promise<T | null> {
  try {
    return (await params.client.readContract({
      address: params.address,
      abi: params.abi,
      functionName: params.functionName,
      args: params.args
    })) as T;
  } catch {
    return null;
  }
}

function sanitizeOptionalString(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function normalizeTransferAddress(address: Address | undefined): string | null {
  if (!address) {
    return null;
  }

  const normalized = normalizeWalletAddress(address);
  return normalized === zeroWalletAddress ? null : normalized;
}

function topicToAddress(topic: string): Address | undefined {
  if (!topic.startsWith("0x") || topic.length !== 66) {
    return undefined;
  }

  return getAddress(`0x${topic.slice(-40)}`);
}

type Erc1155SingleLogs = Awaited<ReturnType<typeof fetchErc1155TransferSingleLogs>>;
type Erc1155BatchLogs = Awaited<ReturnType<typeof fetchErc1155TransferBatchLogs>>;

async function fetchErc1155TransferSingleLogs(params: {
  client: PublicClient;
  address: Address;
  fromBlock: bigint;
  toBlock: bigint;
}) {
  return params.client.getLogs({
    address: params.address,
    event: erc1155TransferSingleEvent,
    fromBlock: params.fromBlock,
    toBlock: params.toBlock
  });
}

async function fetchErc1155TransferBatchLogs(params: {
  client: PublicClient;
  address: Address;
  fromBlock: bigint;
  toBlock: bigint;
}) {
  return params.client.getLogs({
    address: params.address,
    event: erc1155TransferBatchEvent,
    fromBlock: params.fromBlock,
    toBlock: params.toBlock
  });
}

async function readErc1155LogsForWindowWithFallback(params: {
  client: PublicClient;
  address: Address;
  fromBlock: bigint;
  toBlock: bigint;
}): Promise<{
  singleLogs: Erc1155SingleLogs;
  batchLogs: Erc1155BatchLogs;
}> {
  try {
    const [singleLogs, batchLogs] = await Promise.all([
      fetchErc1155TransferSingleLogs(params),
      fetchErc1155TransferBatchLogs(params)
    ]);

    return {
      singleLogs,
      batchLogs
    };
  } catch (error) {
    if (params.fromBlock >= params.toBlock) {
      console.warn("[chain] failed to read ERC-1155 logs for block", {
        blockNumber: Number(params.fromBlock),
        address: params.address,
        error
      });

      return {
        singleLogs: [],
        batchLogs: []
      };
    }

    const middleBlock = params.fromBlock + (params.toBlock - params.fromBlock) / 2n;
    const [leftWindow, rightWindow] = await Promise.all([
      readErc1155LogsForWindowWithFallback({
        client: params.client,
        address: params.address,
        fromBlock: params.fromBlock,
        toBlock: middleBlock
      }),
      readErc1155LogsForWindowWithFallback({
        client: params.client,
        address: params.address,
        fromBlock: middleBlock + 1n,
        toBlock: params.toBlock
      })
    ]);

    return {
      singleLogs: [...leftWindow.singleLogs, ...rightWindow.singleLogs],
      batchLogs: [...leftWindow.batchLogs, ...rightWindow.batchLogs]
    };
  }
}

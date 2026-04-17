import { createHash, createHmac } from "node:crypto";
import process from "node:process";
import { loadLocalEnvFiles } from "@nft-platform/runtime";

loadLocalEnvFiles();

type ApiResponse<T> = {
  status: number;
  data: T;
};

const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();

async function main(): Promise<void> {
  const erc1155Collection = await signedRequest<{
    ok: boolean;
    item: {
      standard: string;
      name: string | null;
      symbol: string | null;
      contractAddress: string;
    };
  }>("GET", "/api/v1/collections/11155111/0x2f2a217caa0948bca6df8de110ce41720c51028e");

  expect(erc1155Collection.status === 200, "ERC-1155 collection request failed.");
  expect(erc1155Collection.data.item.standard === "erc1155", "Expected ERC-1155 collection standard.");
  expect(erc1155Collection.data.item.name === "My Happy Tent", "Unexpected ERC-1155 collection name.");
  expect(erc1155Collection.data.item.symbol === "MHT", "Unexpected ERC-1155 collection symbol.");

  const erc1155Token = await signedRequest<{
    ok: boolean;
    item: {
      standard: string;
      tokenId: string;
      name: string | null;
      mediaStatus: string;
    };
  }>("GET", "/api/v1/tokens/11155111/0x2f2a217caa0948bca6df8de110ce41720c51028e/4");

  expect(erc1155Token.status === 200, "ERC-1155 token request failed.");
  expect(erc1155Token.data.item.standard === "erc1155", "Expected ERC-1155 token standard.");
  expect(erc1155Token.data.item.tokenId === "4", "Unexpected ERC-1155 token id.");
  expect(Boolean(erc1155Token.data.item.name?.includes("Workation")), "Unexpected ERC-1155 token name.");

  const erc1155Owners = await signedRequest<{
    ok: boolean;
    standard: string;
    items: unknown[];
  }>("GET", "/api/v1/owners/11155111/0x2f2a217caa0948bca6df8de110ce41720c51028e/4?limit=10");

  expect(erc1155Owners.status === 200, "ERC-1155 owners request failed.");
  expect(erc1155Owners.data.standard === "erc1155", "Expected ERC-1155 owners response.");
  expect(Array.isArray(erc1155Owners.data.items), "ERC-1155 owners items must be an array.");

  const erc721Collection = await signedRequest<{
    ok: boolean;
    item: {
      standard: string;
      name: string | null;
      symbol: string | null;
      contractAddress: string;
    };
  }>("GET", "/api/v1/collections/11155111/0x41655ae49482de69eec8f6875c34a8ada01965e2");

  expect(erc721Collection.status === 200, "ERC-721 collection request failed.");
  expect(erc721Collection.data.item.standard === "erc721", "Expected ERC-721 collection standard.");
  expect(erc721Collection.data.item.name === "People of History - Bolivar", "Unexpected ERC-721 collection name.");
  expect(erc721Collection.data.item.symbol === "PoHB", "Unexpected ERC-721 collection symbol.");

  const erc721Token = await signedRequest<{
    ok: boolean;
    item: {
      standard: string;
      tokenId: string;
      name: string | null;
      mediaStatus: string;
      media: {
        image: { cdnUrlOriginal: string | null } | null;
      };
    };
  }>("GET", "/api/v1/tokens/11155111/0x41655ae49482de69eec8f6875c34a8ada01965e2/359");

  expect(erc721Token.status === 200, "ERC-721 token request failed.");
  expect(erc721Token.data.item.standard === "erc721", "Expected ERC-721 token standard.");
  expect(erc721Token.data.item.tokenId === "359", "Unexpected ERC-721 token id.");
  expect(erc721Token.data.item.name === "People of History #359", "Unexpected ERC-721 token name.");
  expect(erc721Token.data.item.mediaStatus === "ready", "Expected ERC-721 media to be ready.");
  expect(Boolean(erc721Token.data.item.media.image?.cdnUrlOriginal), "Expected ERC-721 image media.");

  const erc721Owners = await signedRequest<{
    ok: boolean;
    standard: string;
    items: Array<{
      ownerAddress: string;
    }>;
  }>("GET", "/api/v1/owners/11155111/0x41655ae49482de69eec8f6875c34a8ada01965e2/359?limit=10");

  expect(erc721Owners.status === 200, "ERC-721 owners request failed.");
  expect(erc721Owners.data.standard === "erc721", "Expected ERC-721 owners response.");
  expect(erc721Owners.data.items.length === 1, "Expected exactly one ERC-721 owner.");
  expect(
    erc721Owners.data.items[0]?.ownerAddress === "0xf034e8ad11f249c8081d9da94852be1734bc11a4",
    "Unexpected ERC-721 owner address."
  );

  const walletInventory = await signedRequest<{
    ok: boolean;
    standard: string;
    items: Array<{
      standard: string;
      contractAddress: string;
      tokenId: string;
      token: {
        name: string | null;
      } | null;
    }>;
  }>("GET", "/api/v1/owners/wallets/11155111/0xf034e8ad11f249c8081d9da94852be1734bc11a4?standard=erc721&q=359&limit=10");

  expect(walletInventory.status === 200, "Wallet inventory request failed.");
  expect(walletInventory.data.standard === "erc721", "Expected ERC-721 wallet inventory mode.");
  expect(walletInventory.data.items.length >= 1, "Expected at least one wallet inventory item.");
  expect(walletInventory.data.items[0]?.tokenId === "359", "Expected wallet inventory token 359.");

  console.log(
    JSON.stringify(
      {
        ok: true,
        checks: [
          "erc1155_collection",
          "erc1155_token",
          "erc1155_owners",
          "erc721_collection",
          "erc721_token",
          "erc721_owners",
          "wallet_inventory_erc721"
        ],
        apiBaseUrl
      },
      null,
      2
    )
  );
}

async function signedRequest<T>(method: string, path: string): Promise<ApiResponse<T>> {
  const url = new URL(path, apiBaseUrl);
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const body = "";
  const payload = [
    method,
    `${url.pathname}${url.search}`,
    createHash("sha256").update(body).digest("hex"),
    timestamp
  ].join("\n");
  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      "x-client-id": requiredEnv("API_BOOTSTRAP_CLIENT_ID"),
      "x-api-key": requiredEnv("API_BOOTSTRAP_KEY"),
      "x-signature": createHmac("sha256", requiredEnv("API_BOOTSTRAP_SECRET")).update(payload).digest("hex"),
      "x-timestamp": timestamp
    }
  });
  const text = await response.text();
  const data = JSON.parse(text) as T;

  return {
    status: response.status,
    data
  };
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function expect(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

main().catch((error) => {
  console.error("[smoke:fixtures] failed", error);
  process.exit(1);
});
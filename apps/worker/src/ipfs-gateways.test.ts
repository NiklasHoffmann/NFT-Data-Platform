import { buildIpfsGatewayUrls, defaultIpfsGatewayHosts, normalizeAssetUri, parseIpfsGatewayHosts } from "@nft-platform/chain";
import { describe, expect, it } from "vitest";

const cid = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB";
const hosts = ["gw-one.example", "gw-two.example"];

describe("parseIpfsGatewayHosts", () => {
  it("falls back to the public defaults when unset or blank", () => {
    expect(parseIpfsGatewayHosts(undefined)).toEqual([...defaultIpfsGatewayHosts]);
    expect(parseIpfsGatewayHosts("")).toEqual([...defaultIpfsGatewayHosts]);
    expect(parseIpfsGatewayHosts("  ,  ")).toEqual([...defaultIpfsGatewayHosts]);
  });

  it("accepts bare hosts and full origins, and preserves order", () => {
    expect(parseIpfsGatewayHosts("gw-one.example, https://gw-two.example/")).toEqual(hosts);
  });

  it("drops duplicates and unparseable entries", () => {
    expect(parseIpfsGatewayHosts("gw-one.example, gw-one.example, ://nonsense")).toEqual([
      "gw-one.example"
    ]);
  });

  it("never returns a gateway that was retired upstream", () => {
    // ipfs.io and dweb.link serve a service-worker-only notice to server-side
    // requests, and cloudflare-ipfs.com no longer accepts connections at all.
    const retired = ["ipfs.io", "dweb.link", "cloudflare-ipfs.com"];
    expect(parseIpfsGatewayHosts(undefined).some((host) => retired.includes(host))).toBe(false);
  });
});

describe("buildIpfsGatewayUrls", () => {
  it("returns nothing for non-IPFS input, so callers can concatenate", () => {
    expect(buildIpfsGatewayUrls("https://example.com/image.png", hosts)).toEqual([]);
    expect(buildIpfsGatewayUrls("ar://abc", hosts)).toEqual([]);
    expect(buildIpfsGatewayUrls("ipfs://", hosts)).toEqual([]);
  });

  it("expands a bare CID across every configured gateway", () => {
    expect(buildIpfsGatewayUrls(`ipfs://${cid}`, hosts)).toEqual([
      `https://gw-one.example/ipfs/${cid}`,
      `https://gw-two.example/ipfs/${cid}`
    ]);
  });

  it("keeps an explicit ipfs/ or ipns/ prefix instead of nesting it", () => {
    expect(buildIpfsGatewayUrls(`ipfs://ipfs/${cid}/meta.json`, hosts)).toEqual([
      `https://gw-one.example/ipfs/${cid}/meta.json`,
      `https://gw-two.example/ipfs/${cid}/meta.json`
    ]);
    expect(buildIpfsGatewayUrls("ipfs://ipns/example.eth", hosts)).toEqual([
      "https://gw-one.example/ipns/example.eth",
      "https://gw-two.example/ipns/example.eth"
    ]);
  });

  it("tries both routes when the namespace is not recognisably a CID", () => {
    expect(buildIpfsGatewayUrls("ipfs://example.eth/1.json", hosts)).toEqual([
      "https://gw-one.example/ipns/example.eth/1.json",
      "https://gw-one.example/ipfs/example.eth/1.json",
      "https://gw-two.example/ipns/example.eth/1.json",
      "https://gw-two.example/ipfs/example.eth/1.json"
    ]);
  });

  it("tolerates leading slashes after the scheme", () => {
    expect(buildIpfsGatewayUrls(`ipfs:///${cid}`, hosts)).toEqual([
      `https://gw-one.example/ipfs/${cid}`,
      `https://gw-two.example/ipfs/${cid}`
    ]);
  });
});

describe("normalizeAssetUri", () => {
  it("resolves an IPFS URI to the first configured gateway", () => {
    expect(normalizeAssetUri(`ipfs://${cid}`, { gatewayHosts: hosts })).toBe(
      `https://gw-one.example/ipfs/${cid}`
    );
  });

  it("maps Arweave URIs to arweave.net", () => {
    expect(normalizeAssetUri("ar://abc123")).toBe("https://arweave.net/abc123");
  });

  it("leaves http and unknown schemes untouched", () => {
    expect(normalizeAssetUri("https://example.com/a.json")).toBe("https://example.com/a.json");
    expect(normalizeAssetUri("data:application/json,{}")).toBe("data:application/json,{}");
  });

  it("returns the input unchanged when an IPFS URI carries no payload", () => {
    expect(normalizeAssetUri("ipfs://", { gatewayHosts: hosts })).toBe("ipfs://");
  });
});

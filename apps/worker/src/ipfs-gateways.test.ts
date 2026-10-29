import {
  buildIpfsGatewayUrls,
  buildIpfsRetryUrls,
  defaultIpfsGatewayHosts,
  normalizeAssetUri,
  parseIpfsGatewayHosts,
  parseIpfsGatewayUrl
} from "@nft-platform/chain";
import { describe, expect, it } from "vitest";

const cid = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB";
const cid32 = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
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

describe("parseIpfsGatewayUrl", () => {
  it("reads the path style shape", () => {
    expect(parseIpfsGatewayUrl(`https://dweb.link/ipfs/${cid}/image.png`)).toEqual({
      namespace: "ipfs",
      path: `${cid}/image.png`,
      search: ""
    });
  });

  it("reads the subdomain style shape, where the CID sits in the host", () => {
    expect(parseIpfsGatewayUrl(`https://${cid32}.ipfs.dweb.link/image.png`)).toEqual({
      namespace: "ipfs",
      path: `${cid32}/image.png`,
      search: ""
    });
  });

  it("treats a bare subdomain host as having no sub-path", () => {
    expect(parseIpfsGatewayUrl(`https://${cid32}.ipfs.dweb.link`)).toEqual({
      namespace: "ipfs",
      path: cid32,
      search: ""
    });
  });

  it("keeps the query string, which some gateways depend on", () => {
    expect(parseIpfsGatewayUrl(`https://dweb.link/ipfs/${cid}?filename=a.png`)?.search).toBe(
      "?filename=a.png"
    );
  });

  it("handles ipns in both shapes", () => {
    expect(parseIpfsGatewayUrl("https://dweb.link/ipns/example.eth")?.namespace).toBe("ipns");
    expect(parseIpfsGatewayUrl("https://example-eth.ipns.dweb.link")?.namespace).toBe("ipns");
  });

  it("returns null for anything that does not reference IPFS content", () => {
    expect(parseIpfsGatewayUrl("https://example.com/image.png")).toBeNull();
    expect(parseIpfsGatewayUrl("https://dweb.link/")).toBeNull();
    expect(parseIpfsGatewayUrl("not a url")).toBeNull();
  });
});

describe("buildIpfsRetryUrls", () => {
  it("rewrites a path style gateway URL onto every configured gateway", () => {
    expect(buildIpfsRetryUrls(`https://dweb.link/ipfs/${cid}`, hosts)).toEqual([
      `https://gw-one.example/ipfs/${cid}`,
      `https://gw-two.example/ipfs/${cid}`
    ]);
  });

  it("rewrites a subdomain style gateway URL to path style, which is the bug this covers", () => {
    expect(buildIpfsRetryUrls(`https://${cid32}.ipfs.dweb.link/image.png`, hosts)).toEqual([
      `https://gw-one.example/ipfs/${cid32}/image.png`,
      `https://gw-two.example/ipfs/${cid32}/image.png`
    ]);
  });

  it("yields nothing for a plain HTTP media URL, so the source stays the only candidate", () => {
    expect(buildIpfsRetryUrls("https://cdn.example.com/a.png", hosts)).toEqual([]);
  });
});

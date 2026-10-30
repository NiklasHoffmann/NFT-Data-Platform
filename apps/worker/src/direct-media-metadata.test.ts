import { describe, expect, it } from "vitest";
import { buildDirectMediaMetadata } from "./jobs/processors";

const sourceUrl = "https://gateway.example/ipfs/bafyexample";

describe("buildDirectMediaMetadata", () => {
  it("treats an image response as the artwork, which JSON parsing used to choke on", () => {
    const result = buildDirectMediaMetadata({ contentType: "image/jpeg", sourceUrl });

    expect(result?.imageOriginalUrl).toBe(sourceUrl);
    expect(result?.hasDownloadableMedia).toBe(true);
    expect(result?.animationOriginalUrl).toBeNull();
  });

  it("maps video to the animation slot and audio to its own", () => {
    expect(buildDirectMediaMetadata({ contentType: "video/mp4", sourceUrl })?.animationOriginalUrl).toBe(
      sourceUrl
    );
    expect(buildDirectMediaMetadata({ contentType: "audio/mpeg", sourceUrl })?.audioOriginalUrl).toBe(
      sourceUrl
    );
  });

  it("ignores parameters after the media type", () => {
    expect(buildDirectMediaMetadata({ contentType: "image/png; charset=binary", sourceUrl })).not.toBeNull();
  });

  it("leaves a JSON document to the normal parsing path", () => {
    expect(buildDirectMediaMetadata({ contentType: "application/json", sourceUrl })).toBeNull();
    expect(buildDirectMediaMetadata({ contentType: "text/plain", sourceUrl })).toBeNull();
  });

  it("leaves a response without a content type to the normal path", () => {
    // A server that forgot the header might still be serving JSON, so guessing media
    // here would turn a readable document into a bogus image reference.
    expect(buildDirectMediaMetadata({ contentType: null, sourceUrl })).toBeNull();
  });

  it("derives a stable hash from the synthesised payload, not from the file", () => {
    const first = buildDirectMediaMetadata({ contentType: "image/jpeg", sourceUrl });
    const second = buildDirectMediaMetadata({ contentType: "image/jpeg", sourceUrl });
    const other = buildDirectMediaMetadata({ contentType: "image/jpeg", sourceUrl: `${sourceUrl}/2` });

    expect(first?.payloadHash).toBe(second?.payloadHash);
    expect(first?.payloadHash).not.toBe(other?.payloadHash);
  });
});

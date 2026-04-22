import {
  findMediaAssetsByIds,
  findCollectionByIdentity,
  findErc721OwnershipByToken,
  findTokenByIdentity,
  getMongoCollections,
  listErc721OwnedTokenIdsForCollection,
  listErc1155Balances,
  listTokens,
  serializeCollectionDocument,
  serializeErc1155BalanceDocument,
  serializeErc721OwnershipDocument,
  serializeJobDocument,
  serializeMediaAssetDocument,
  serializeTokenDocument,
  type TokenDocument
} from "@nft-platform/db";
import { evmAddressSchema, evmTokenIdSchema, normalizeContractAddress, supportedChains } from "@nft-platform/domain";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { serializeEnrichedCollection } from "../lib/collection-response";
import { getWebRuntimeConfig } from "../lib/env";
import { getWebMongoDatabase } from "../lib/mongodb";
import { loadOperationsHealth, type OperationsHealthSnapshot } from "../lib/operations-health";
import { serializeEnrichedToken } from "../lib/token-response";
import { DiscoverLiveRefresh } from "./discover-live-refresh";
import { DiscoverSubmitButton } from "./discover-submit-button";
import { InteractiveMediaPreviewGallery, type MediaGalleryItem, type MediaActionLink } from "./media-preview-gallery";
import { ProgressiveCardGrid } from "./progressive-card-grid";

const tokenScopedJobTypes = ["refresh-token", "refresh-media"] as const;
const collectionScopedJobTypes = ["refresh-collection", "reindex-range"] as const;
const homeViewSchema = z.enum(["nft", "collection", "jobs", "raw", "operations"]);
const webRuntimeConfig = getWebRuntimeConfig();

export const dynamic = "force-dynamic";

const homeQuerySchema = z.object({
  chainId: z.coerce.number().int().positive().optional(),
  contractAddress: evmAddressSchema.optional(),
  tokenId: evmTokenIdSchema.optional(),
  view: homeViewSchema.optional(),
  status: z.enum(["loaded", "queued", "invalid", "not-found", "unresolved", "failed"]).optional(),
  message: z.string().trim().min(1).optional()
});

type HomeView = z.infer<typeof homeViewSchema>;

type CollectionTokenCardData = {
  token: ReturnType<typeof serializeTokenDocument>;
  tokenId: string;
  name: string | null;
  supplyQuantity: string | null;
  mediaStatus: string;
  updatedAt: Date | string;
  mediaAssets: Array<ReturnType<typeof serializeMediaAssetDocument>>;
};

const emptyOperationsHealth: OperationsHealthSnapshot = {
  summary: {
    activeJobsCount: 0,
    retryingMediaJobsCount: 0,
    failedJobsCount: 0,
    laggingCollectionsCount: 0
  },
  activeJobs: [],
  retryingMediaJobs: [],
  recentFailedJobs: [],
  laggingCollections: []
};

export default async function HomePage(props: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const rawSearchParams = props.searchParams ? await props.searchParams : undefined;
  const parsedQuery = homeQuerySchema.safeParse({
    chainId: firstValue(rawSearchParams?.chainId),
    contractAddress: firstValue(rawSearchParams?.contractAddress),
    tokenId: firstValue(rawSearchParams?.tokenId),
    view: firstValue(rawSearchParams?.view),
    status: firstValue(rawSearchParams?.status),
    message: firstValue(rawSearchParams?.message)
  });
  const initialChainId = parsedQuery.success ? parsedQuery.data.chainId ?? 11155111 : 11155111;
  const initialContractAddress = parsedQuery.success ? parsedQuery.data.contractAddress ?? "" : "";
  const initialTokenId = parsedQuery.success ? parsedQuery.data.tokenId ?? "" : "";
  const activeView: HomeView = parsedQuery.success ? parsedQuery.data.view ?? "nft" : "nft";
  const bannerStatus = parsedQuery.success ? parsedQuery.data.status : "invalid";
  const bannerMessage = parsedQuery.success
    ? parsedQuery.data.message
    : "Enter a valid contract and optional token id to discover collection and NFT data from the local read model.";
  const shouldLoadIdentityData = parsedQuery.success && Boolean(parsedQuery.data.contractAddress);
  const shouldLoadOperationsHealth = activeView === "operations";
  const shouldLoadHomepageTokenPreviews = activeView === "nft" && !initialContractAddress && !initialTokenId;
  const database = shouldLoadIdentityData || shouldLoadOperationsHealth || shouldLoadHomepageTokenPreviews ? getWebMongoDatabase() : null;
  let operationsHealth = emptyOperationsHealth;
  let operationsHealthWarning: string | null = null;

  if (database && shouldLoadOperationsHealth) {
    try {
      operationsHealth = await loadOperationsHealth(database);
    } catch {
      operationsHealthWarning = "Operations telemetry is temporarily unavailable while MongoDB or Redis finishes starting up.";
    }
  }

  let collection: Awaited<ReturnType<typeof serializeEnrichedCollection>> | null = null;
  let token: Awaited<ReturnType<typeof serializeEnrichedToken>> | null = null;
  let collectionRecord: ReturnType<typeof serializeCollectionDocument> | null = null;
  let tokenRecord: ReturnType<typeof serializeTokenDocument> | null = null;
  let erc721OwnershipRecord: ReturnType<typeof serializeErc721OwnershipDocument> | null = null;
  let erc1155BalanceRecords: Array<ReturnType<typeof serializeErc1155BalanceDocument>> = [];
  let mediaAssetRecords: Array<ReturnType<typeof serializeMediaAssetDocument>> = [];
  let metadataVersionRecords: Array<Record<string, unknown>> = [];
  let jobRecords: Array<ReturnType<typeof serializeJobDocument>> = [];
  let collectionTokenCards: CollectionTokenCardData[] = [];
  let randomTokenCards: CollectionTokenCardData[] = [];
  let homepageTokenCards: CollectionTokenCardData[] = [];
  let pollingReason: string | null = null;

  if (database && shouldLoadHomepageTokenPreviews) {
    try {
      const previewTokens = await listTokens({
        database,
        limit: 9
      });
      homepageTokenCards = await buildTokenCardData(database, previewTokens);
    } catch {
      homepageTokenCards = [];
    }
  }

  if (parsedQuery.success && parsedQuery.data.contractAddress && database) {
    const normalizedContractAddress = normalizeContractAddress(parsedQuery.data.contractAddress);
    const collections = getMongoCollections(database);
    const [collectionDocument, tokenDocument, relatedJobs] = await Promise.all([
      findCollectionByIdentity({
        database,
        chainId: parsedQuery.data.chainId ?? 11155111,
        contractAddress: normalizedContractAddress
      }),
      parsedQuery.data.tokenId
        ? findTokenByIdentity({
            database,
            chainId: parsedQuery.data.chainId ?? 11155111,
            contractAddress: normalizedContractAddress,
            tokenId: parsedQuery.data.tokenId
          })
        : Promise.resolve(null),
      collections.jobs
        .find({
          $or: [
            ...(parsedQuery.data.tokenId
              ? [
                  {
                    type: { $in: [...tokenScopedJobTypes] },
                    "payload.chainId": parsedQuery.data.chainId ?? 11155111,
                    "payload.contractAddress": normalizedContractAddress,
                    "payload.tokenId": parsedQuery.data.tokenId
                  }
                ]
              : []),
            {
              type: { $in: [...collectionScopedJobTypes] },
              "payload.chainId": parsedQuery.data.chainId ?? 11155111,
              "payload.contractAddress": normalizedContractAddress
            }
          ]
        })
        .sort({ updatedAt: -1, _id: -1 })
        .limit(12)
        .toArray()
    ]);

    collectionRecord = collectionDocument ? serializeCollectionDocument(collectionDocument) : null;
    tokenRecord = tokenDocument ? serializeTokenDocument(tokenDocument) : null;
    jobRecords = relatedJobs.map(serializeJobDocument);

    if (collectionDocument) {
      collection = await serializeEnrichedCollection(database, collectionDocument);

      if (collectionDocument.indexedTokenCount > 0) {
        const [collectionTokens, ownedTokenIds] = await Promise.all([
          listTokens({
            database,
            chainId: parsedQuery.data.chainId ?? 11155111,
            contractAddress: normalizedContractAddress,
            limit: collectionDocument.indexedTokenCount
          }),
          collectionDocument.standard === "erc721"
            ? listErc721OwnedTokenIdsForCollection({
                database,
                chainId: parsedQuery.data.chainId ?? 11155111,
                contractAddress: normalizedContractAddress
              })
            : Promise.resolve(null)
        ]);
        const ownedTokenIdSet = ownedTokenIds ? new Set(ownedTokenIds) : null;
        const visibleCollectionTokens = collectionTokens.filter((collectionToken) => {
          if (collectionDocument.standard === "erc721") {
            return ownedTokenIdSet ? ownedTokenIdSet.has(collectionToken.tokenId) : false;
          }

          return collectionToken.supplyQuantity !== "0";
        });
        collectionTokenCards = await buildTokenCardData(database, visibleCollectionTokens);
      }
    }

    if (tokenDocument && parsedQuery.data.tokenId) {
      if (activeView === "nft") {
        const randomTokenDocuments = await collections.tokens.aggregate<TokenDocument>([
          {
            $match: {
              contractAddress: {
                $ne: normalizedContractAddress
              }
            }
          },
          {
            $sample: {
              size: 18
            }
          }
        ]).toArray();
        randomTokenCards = await buildTokenCardData(database, randomTokenDocuments);
      }

      token = await serializeEnrichedToken(database, tokenDocument);

      const [erc721OwnershipDocument, erc1155Balances, metadataVersions, mediaAssets] = await Promise.all([
        findErc721OwnershipByToken({
          database,
          chainId: parsedQuery.data.chainId ?? 11155111,
          contractAddress: normalizedContractAddress,
          tokenId: parsedQuery.data.tokenId
        }),
        listErc1155Balances({
          database,
          chainId: parsedQuery.data.chainId ?? 11155111,
          contractAddress: normalizedContractAddress,
          tokenId: parsedQuery.data.tokenId,
          limit: 100
        }),
        collections.metadataVersions
          .find({ tokenRef: tokenDocument._id })
          .sort({ version: -1, fetchedAt: -1 })
          .limit(10)
          .toArray(),
        collections.mediaAssets.find({ tokenRef: tokenDocument._id }).sort({ updatedAt: -1, _id: -1 }).toArray()
      ]);

      erc721OwnershipRecord = erc721OwnershipDocument
        ? serializeErc721OwnershipDocument(erc721OwnershipDocument)
        : null;
      erc1155BalanceRecords = erc1155Balances.map(serializeErc1155BalanceDocument);
      mediaAssetRecords = mediaAssets.map(serializeMediaAssetDocument);
      metadataVersionRecords = metadataVersions.map((document) => ({
        ...document,
        _id: document._id.toHexString(),
        tokenRef: document.tokenRef.toHexString()
      }));
    }

    pollingReason = getPollingReason({
      bannerStatus,
      collection,
      token,
      jobRecords
    });
  }

  const hasCollectionMetadataDetails = collection
    ? [
        collection.externalUrl,
        collection.creatorName,
        collection.creatorAddress,
        collection.collectionMetadataHash,
        collection.lastCollectionMetadataFetchAt,
        collection.lastCollectionMetadataError,
        collection.imageOriginalUrl,
        collection.bannerImageOriginalUrl,
        collection.featuredImageOriginalUrl,
        collection.animationOriginalUrl,
        collection.audioOriginalUrl,
        collection.interactiveOriginalUrl
      ].some(hasMeaningfulValue)
    : false;
  const hasContractSignals = collection
    ? [
        collection.contractOwnerAddress,
        collection.royaltyRecipientAddress,
        collection.royaltyBasisPoints,
        collection.contractUriRaw,
        collection.contractUriResolved
      ].some(hasMeaningfulValue)
    : false;
  const collectionMetadataPayloadMessage = collection
    ? collection.lastCollectionMetadataError
      ? `Collection metadata could not be loaded. ${formatOperatorMetadataError(collection.lastCollectionMetadataError)}`
      : collection.contractUriResolved
        ? "Collection metadata is not currently available from the resolved contract URI."
        : "No resolved contract URI is currently stored for this collection."
    : "No collection metadata payload is currently stored.";

  return (
    <main className="shell shell--discover">
      <section className="discover-top-grid">
        <section className="hero hero--discover hero--compact">
          <div className="discover-hero-copy">
            <p className="eyebrow">Presentation mode</p>
            <h1>NFT Discover</h1>
            <p className="lede">
              Enter a contract and optional token id, queue a discovery refresh on the server, and
              render the current indexed result directly from the local NFT read model.
            </p>
          </div>
        </section>

        <section className="panel discover-panel discover-panel--compact">
          <div className="discover-header">
            <div className="discover-header__copy">
              <h2>Discover collection or token</h2>
              <p className="panel-copy">
                Queue a refresh and keep the latest indexed collection and token state directly in view.
              </p>
            </div>
            <div className="discover-header__status">
              <div className={`status-pill status-pill--${bannerStatus ?? "idle"}`}>
                {bannerStatus === "loaded"
                  ? "Loaded"
                  : bannerStatus === "queued"
                    ? "Queued"
                    : bannerStatus === "not-found"
                      ? "Not found"
                      : bannerStatus === "unresolved"
                        ? "Unresolved"
                        : bannerStatus === "failed"
                          ? "Failed"
                    : bannerStatus === "invalid"
                      ? "Check input"
                      : "Ready"}
              </div>
            </div>
          </div>

          <form action="/discover" method="post" className="discover-form">
            <label className="field">
              <span>Chain</span>
              <select name="chainId" defaultValue={String(initialChainId)}>
                {supportedChains.map((chain) => (
                  <option key={chain.id} value={chain.id}>
                    {chain.name} ({chain.id})
                  </option>
                ))}
              </select>
            </label>

            <label className="field field--wide">
              <span>Contract address</span>
              <input
                name="contractAddress"
                type="text"
                placeholder="0x41655ae49482de69eec8f6875c34a8ada01965e2"
                defaultValue={initialContractAddress}
              />
            </label>

            <label className="field">
              <span>Token id</span>
              <input name="tokenId" type="text" placeholder="359 (optional)" defaultValue={initialTokenId} />
            </label>

            <DiscoverSubmitButton />
          </form>

          <p className="banner-copy">{bannerMessage ?? "Use the fields above to load a collection or token."}</p>
          <DiscoverLiveRefresh active={Boolean(pollingReason)} reason={pollingReason} />
        </section>
      </section>

      <ViewMenu
        activeView={activeView}
        chainId={initialChainId}
        contractAddress={initialContractAddress}
        tokenId={initialTokenId}
        status={bannerStatus}
        message={bannerMessage}
      />

      {activeView === "nft" ? (
        <>
          <section className="panel panel--result panel--token panel--token-primary">
            <h2>Token</h2>
            {token ? (
              <div className="result-stack result-stack--token">
                <section className="token-overview-grid">
                  <div className="token-overview-media">
                    <MediaPreviewGallery token={token} mediaAssets={mediaAssetRecords} defaultActiveIds={["animation", "image"]} />
                  </div>

                  <div className="token-overview-copy">
                    <section className="token-overview-card">
                      <section className="token-summary-card">
                        <div className="metadata-grid metadata-grid--token-summary">
                          <div>
                            <span className="meta-label">Name</span>
                            <strong>{token.name ?? `Token ${token.tokenId}`}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Token id</span>
                            <strong>{token.tokenId}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Metadata</span>
                            <strong>{token.metadataStatus}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Media</span>
                            <strong>{token.mediaStatus}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Standard</span>
                            <strong>{token.standard}</strong>
                          </div>
                          {token.standard === "erc1155" ? (
                            <div>
                              <span className="meta-label">Quantity</span>
                              <strong>{formatNullable(token.supplyQuantity)}</strong>
                            </div>
                          ) : null}
                          <div>
                            <span className="meta-label">Owner state version</span>
                            <strong>{token.ownerStateVersion}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Metadata version</span>
                            <strong>{token.metadataVersion}</strong>
                          </div>
                          <div>
                            <span className="meta-label">Updated</span>
                            <strong>{formatDateValue(token.updatedAt)}</strong>
                          </div>
                        </div>
                        {token.description ? <p className="panel-copy panel-copy--lead">{token.description}</p> : null}
                      </section>

                      <div className="token-facts-stack">
                        <section className="token-details-card">
                          <h3>Token details</h3>
                          <dl className="detail-list detail-list--split">
                            <div>
                              <dt>Collection address</dt>
                              <dd className="address-copy">{token.contractAddress}</dd>
                            </div>
                            <div>
                              <dt>External URL</dt>
                              <dd>{renderDetailActionLinks([{ label: "Open external", href: token.externalUrl }])}</dd>
                            </div>
                            {token.interactiveOriginalUrl ? (
                              <>
                                <div>
                                  <dt>Interactive asset</dt>
                                  <dd>
                                    {renderDetailActionLinks([
                                      { label: "Open interactive", href: token.interactiveOriginalUrl }
                                    ])}
                                  </dd>
                                </div>
                                <div>
                                  <dt>Interactive type</dt>
                                  <dd>{formatNullable(token.interactiveMediaType)}</dd>
                                </div>
                              </>
                            ) : null}
                            <div>
                              <dt>Metadata creator</dt>
                              <dd>{formatNullable(token.creator?.name ?? null)}</dd>
                            </div>
                            <div>
                              <dt>Metadata creator address</dt>
                              <dd>{token.creator?.address ? <span className="address-copy">{token.creator.address}</span> : "-"}</dd>
                            </div>
                            <div>
                              <dt>Metadata creator source</dt>
                              <dd>{formatTokenCreatorSource(token.creator?.source ?? null)}</dd>
                            </div>
                            <div>
                              <dt>Last media process</dt>
                              <dd>{formatDateValue(token.lastMediaProcessAt)}</dd>
                            </div>
                            {token.standard === "erc1155" ? (
                              <div>
                                <dt>On-chain quantity</dt>
                                <dd>{formatNullable(token.supplyQuantity)}</dd>
                              </div>
                            ) : null}
                            <div>
                              <dt>Created</dt>
                              <dd>{formatDateValue(token.createdAt)}</dd>
                            </div>
                          </dl>
                        </section>

                        <details className="technical-panel">
                          <summary className="technical-panel__summary">
                            <div>
                              <strong>Technical metadata</strong>
                              <p>URI plumbing, hashes, fetch timing, and last metadata errors for this token.</p>
                            </div>
                          </summary>
                          <div className="technical-panel__body">
                            <dl className="detail-list detail-list--split">
                              <div>
                                <dt>Metadata URI raw</dt>
                                <dd>{renderDetailActionLinks([{ label: "Open raw metadata", href: token.metadataUriRaw }])}</dd>
                              </div>
                              <div>
                                <dt>Metadata URI mode</dt>
                                <dd>{inferUriMode(token.metadataUriRaw)}</dd>
                              </div>
                              <div>
                                <dt>Metadata URI resolved</dt>
                                <dd>
                                  {renderDetailActionLinks([
                                    { label: "Open resolved metadata", href: token.metadataUriResolved }
                                  ])}
                                </dd>
                              </div>
                              <div>
                                <dt>Metadata hash</dt>
                                <dd className="address-copy">{formatNullable(token.metadataHash)}</dd>
                              </div>
                              <div>
                                <dt>Metadata error</dt>
                                <dd className="detail-copy detail-copy--quiet">{formatOperatorMetadataError(token.lastMetadataError)}</dd>
                              </div>
                              <div>
                                <dt>Last metadata fetch</dt>
                                <dd>{formatDateValue(token.lastMetadataFetchAt)}</dd>
                              </div>
                            </dl>
                          </div>
                        </details>
                      </div>
                    </section>
                  </div>
                </section>

                {bannerStatus === "failed" && bannerMessage ? (
                  <section className="subsection-card subsection-card--warning">
                    <h3>Diagnostics</h3>
                    <dl className="detail-list">
                      <div>
                        <dt>Discover status</dt>
                        <dd>
                          <span className={`inline-status inline-status--${bannerStatus}`}>{bannerStatus}</span>
                        </dd>
                      </div>
                      <div>
                        <dt>Failure detail</dt>
                        <dd className="field-copy field-copy--strong">{bannerMessage}</dd>
                      </div>
                      <div>
                        <dt>Current interpretation</dt>
                        <dd>
                          {token.metadataStatus === "failed"
                            ? "On-chain token data is present, but the metadata endpoint did not return usable JSON."
                            : "The discover flow failed before the read model became complete."}
                        </dd>
                      </div>
                    </dl>
                  </section>
                ) : null}

                <section className="subsection-card">
                  <h3>{token?.standard === "erc1155" ? "Holders and balances" : "Ownership"}</h3>
                  <p className="panel-copy">
                    {token?.standard === "erc1155"
                      ? "ERC-1155 can have multiple holders. Balances below represent quantity per holder and are reconstructed from transfer events."
                      : "ERC-721 has one current owner per token. The value below reflects the latest ownership snapshot."}
                  </p>
                  {erc721OwnershipRecord ? (
                    <dl className="detail-list">
                      <div>
                        <dt>Current owner</dt>
                        <dd className="address-copy">{erc721OwnershipRecord.ownerAddress}</dd>
                      </div>
                      <div>
                        <dt>Updated</dt>
                        <dd>{formatDateValue(erc721OwnershipRecord.updatedAt)}</dd>
                      </div>
                    </dl>
                  ) : erc1155BalanceRecords.length > 0 ? (
                    <>
                      <dl className="detail-list">
                        <div>
                          <dt>Total quantity</dt>
                          <dd>{sumErc1155Balances(erc1155BalanceRecords)}</dd>
                        </div>
                        <div>
                          <dt>Holder count</dt>
                          <dd>{erc1155BalanceRecords.length}</dd>
                        </div>
                      </dl>
                      <div className="table-card">
                        <table>
                          <thead>
                            <tr>
                              <th>Holder address</th>
                              <th>Balance</th>
                              <th>Updated</th>
                            </tr>
                          </thead>
                          <tbody>
                            {erc1155BalanceRecords.map((balance) => (
                              <tr key={balance._id}>
                                <td className="address-copy">{balance.ownerAddress}</td>
                                <td>{balance.balance}</td>
                                <td>{formatDateValue(balance.updatedAt)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  ) : (
                    <p className="empty-state">
                      {token?.standard === "erc1155" && token.supplyQuantity && token.supplyQuantity !== "0"
                        ? "No balance documents are currently stored. This ERC-1155 token exposes on-chain supply, but no reconstructible transfer history is available for holder snapshots."
                        : token?.standard === "erc721"
                          ? "No ERC-721 ownership snapshot is currently stored for this token."
                        : "No owner or balance documents are currently stored."}
                    </p>
                  )}
                </section>

                <section className="subsection-card">
                  <h3>Attributes</h3>
                  {token.attributes.length > 0 ? (
                    <div className="tag-grid">
                      {token.attributes.map((attribute, index) => (
                        <div key={`${attribute.trait_type}-${String(attribute.value)}-${index}`} className="tag-card">
                          <span>{attribute.trait_type}</span>
                          <strong>{String(attribute.value)}</strong>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="empty-state">No token attributes are currently stored.</p>
                  )}
                </section>

                <section className="subsection-card">
                  <h3>Media assets</h3>
                  {mediaAssetRecords.length > 0 ? (
                    <div className="asset-grid">
                      {mediaAssetRecords.map((asset) => (
                        <div key={asset._id} className="asset-card">
                          {(() => {
                            const assetKind = asset.kind === "image" || asset.kind === "animation" || asset.kind === "audio" ? asset.kind : "animation";
                            const assetRenderUrl = asset.cdnUrlOriginal ?? asset.cdnUrlOptimized ?? asset.sourceUrl;
                            const assetStatus =
                              getDisplayMediaAssetStatus(asset.status, asset.statusDetail, {
                                sourceUrl: asset.sourceUrl,
                                renderUrl: assetRenderUrl,
                                kind: assetKind,
                                mimeType: asset.mimeType,
                                hasLocalMirror: Boolean(asset.cdnUrlOriginal ?? asset.cdnUrlOptimized ?? asset.cdnUrlThumbnail)
                              }) ?? asset.status;

                            return (
                              <div className="asset-card__header">
                                <strong>{asset.kind}</strong>
                                <span className={`inline-status inline-status--${assetStatus}`}>{formatStatusLabel(assetStatus)}</span>
                              </div>
                            );
                          })()}
                          <dl className="detail-list">
                            <div>
                              <dt>Source URL</dt>
                              <dd>{renderDetailActionLinks([{ label: "Open source", href: asset.sourceUrl }])}</dd>
                            </div>
                            <div>
                              <dt>Original CDN</dt>
                              <dd>{renderDetailActionLinks([{ label: "Open original", href: asset.cdnUrlOriginal }])}</dd>
                            </div>
                            <div>
                              <dt>Optimized CDN</dt>
                              <dd>{renderDetailActionLinks([{ label: "Open optimized", href: asset.cdnUrlOptimized }])}</dd>
                            </div>
                            <div>
                              <dt>Thumbnail CDN</dt>
                              <dd>{renderDetailActionLinks([{ label: "Open thumbnail", href: asset.cdnUrlThumbnail }])}</dd>
                            </div>
                            <div>
                              <dt>MIME type</dt>
                              <dd>{formatNullable(asset.mimeType)}</dd>
                            </div>
                            <div>
                              <dt>Size bytes</dt>
                              <dd>{formatNullable(asset.sizeBytes)}</dd>
                            </div>
                            <div>
                              <dt>Dimensions</dt>
                              <dd>{formatDimensions(asset.width, asset.height)}</dd>
                            </div>
                            <div>
                              <dt>Duration</dt>
                              <dd>{formatNullable(asset.durationSec)}</dd>
                            </div>
                            <div>
                              <dt>Status detail</dt>
                              <dd>{formatNullable(asset.statusDetail)}</dd>
                            </div>
                          </dl>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="empty-state">No media asset documents are currently stored.</p>
                  )}
                </section>

                <section className="subsection-card">
                  <h3>Token metadata payload</h3>
                  {token.metadataPayload ? (
                    <JsonPanel title="Token metadata JSON" data={token.metadataPayload} compact />
                  ) : token.metadataUriResolved ? (
                    <p className="empty-state empty-state--metadata">
                      {token.lastMetadataError
                        ? `Token metadata could not be loaded. ${formatOperatorMetadataError(token.lastMetadataError)}`
                        : "Token metadata is not currently available from the resolved metadata URI."}
                    </p>
                  ) : (
                    <p className="empty-state">No token metadata payload is currently stored.</p>
                  )}
                </section>

                {randomTokenCards.length > 0 ? (
                  <section className="subsection-card">
                    <h3>Random known tokens</h3>
                    <p className="panel-copy">Random sample from other tokens already stored in the read model.</p>
                    <ProgressiveCardGrid
                      className="asset-grid collection-token-grid known-token-grid"
                      initialCount={3}
                      increment={3}
                      buttonLabel="Show more random tokens"
                      remainingLabel="more random tokens"
                    >
                      {randomTokenCards.map((randomTokenCard) =>
                        renderCollectionTokenCard({
                          collectionToken: randomTokenCard,
                          collectionStandard: randomTokenCard.token.standard
                        })
                      )}
                    </ProgressiveCardGrid>
                  </section>
                ) : null}
              </div>
            ) : initialTokenId ? (
              <div className="result-stack">
                <section
                  className={`subsection-card${bannerStatus === "not-found" || bannerStatus === "failed" ? " subsection-card--warning" : ""}`}
                >
                  <h3>{getTokenLookupHeading(bannerStatus)}</h3>
                  <dl className="detail-list detail-list--split">
                    <div>
                      <dt>Requested token id</dt>
                      <dd>{initialTokenId}</dd>
                    </div>
                    <div>
                      <dt>Contract</dt>
                      <dd className="address-copy">{initialContractAddress}</dd>
                    </div>
                    <div>
                      <dt>Collection state</dt>
                      <dd>{collection ? "confirmed" : "not confirmed yet"}</dd>
                    </div>
                    <div>
                      <dt>Token state</dt>
                      <dd>{getTokenLookupStateLabel(bannerStatus)}</dd>
                    </div>
                  </dl>
                  <p className="field-copy field-copy--strong">
                    {getTokenLookupMessage({
                      bannerStatus,
                      bannerMessage,
                      hasCollection: Boolean(collection)
                    })}
                  </p>
                  <p className="panel-copy">
                    {collection
                      ? "Collection-level public data is still shown below even though no token document is currently available for this id."
                      : "If the contract address can be confirmed, collection-level public data will still load even when no token document is available for this id."}
                  </p>
                </section>

                {collection && collectionTokenCards.length > 0 ? (
                  <section className="subsection-card">
                    <h3>Known tokens from this collection</h3>
                    <p className="panel-copy">
                      This token is not materialized yet, but you can open already indexed tokens from the same collection.
                    </p>
                    <ProgressiveCardGrid
                      className="asset-grid collection-token-grid"
                      initialCount={3}
                      increment={3}
                      buttonLabel="Load more tokens"
                      remainingLabel="more known tokens"
                    >
                      {collectionTokenCards.map((collectionToken) =>
                        renderCollectionTokenCard({
                          collectionToken,
                          collectionStandard: collection.standard
                        })
                      )}
                    </ProgressiveCardGrid>
                  </section>
                ) : null}
              </div>
            ) : (
              <div className="result-stack">
                <p className="empty-state token-empty-state">
                  No token selected yet. Enter a token id above to inspect a specific NFT inside this collection.
                </p>
                {collection && collectionTokenCards.length > 0 ? (
                  <section className="subsection-card">
                    <h3>Known tokens from this collection</h3>
                    <p className="panel-copy">
                      You can jump straight into any token that is already indexed for this collection.
                    </p>
                    <ProgressiveCardGrid
                      className="asset-grid collection-token-grid"
                      initialCount={3}
                      increment={3}
                      buttonLabel="Load more tokens"
                      remainingLabel="more known tokens"
                    >
                      {collectionTokenCards.map((collectionToken) =>
                        renderCollectionTokenCard({
                          collectionToken,
                          collectionStandard: collection.standard
                        })
                      )}
                    </ProgressiveCardGrid>
                  </section>
                ) : homepageTokenCards.length > 0 ? (
                  <section className="subsection-card">
                    <h3>Recently indexed tokens</h3>
                    <p className="panel-copy">
                      The read model already contains tokens. You can open one directly or start a fresh discover lookup above.
                    </p>
                    <ProgressiveCardGrid
                      className="asset-grid collection-token-grid known-token-grid"
                      initialCount={3}
                      increment={3}
                      buttonLabel="Show more indexed tokens"
                      remainingLabel="more indexed tokens"
                    >
                      {homepageTokenCards.map((collectionToken) =>
                        renderCollectionTokenCard({
                          collectionToken,
                          collectionStandard: collectionToken.token.standard
                        })
                      )}
                    </ProgressiveCardGrid>
                  </section>
                ) : null}
              </div>
            )}
          </section>

        </>
        ) : null}

      {activeView === "collection" ? (
        <section className="panel panel--result">
          <h2>Collection</h2>
          {collection ? (
            <div className="result-stack">
              <section className="collection-overview-grid">
                <div className="collection-overview-media">
                  <section className="subsection-card collection-overview-media-card">
                    <h3>Collection media</h3>
                    <CollectionMediaPreviewGallery collection={collection} />
                  </section>
                </div>

                <div className="collection-overview-copy">
                  <section className="collection-identity">
                    <div className="collection-identity__copy">
                      <strong>{collection.name ?? "Unknown collection"}</strong>
                      <p>{collection.description ?? "Collection-level metadata, contract signals, and indexed token coverage."}</p>
                    </div>
                    <div className="collection-identity__meta">
                      <span className="collection-symbol">{collection.symbol ?? "No symbol"}</span>
                      <span className="collection-address address-copy">{collection.contractAddress}</span>
                    </div>
                  </section>

                  <section className="token-overview-card collection-overview-card">
                    <section className="token-summary-card">
                      <div className="metadata-grid metadata-grid--collection-summary">
                        <div>
                          <span className="meta-label">Name</span>
                          <strong>{collection.name ?? "Unknown"}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Standard</span>
                          <strong>{collection.standard}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Symbol</span>
                          <strong>{collection.symbol ?? "-"}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Contract</span>
                          <strong className="address-copy">{collection.contractAddress}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Indexed tokens</span>
                          <strong>{collectionTokenCards.length}</strong>
                        </div>
                        <div>
                          <span className="meta-label">On-chain total supply</span>
                          <strong>{formatNullable(collection.totalSupply)}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Holders</span>
                          <strong>{formatCollectionHolderSummary({ collection, collectionTokenCards })}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Sync status</span>
                          <strong>{collection.syncStatus}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Last indexed block</span>
                          <strong>{formatNullable(collection.lastIndexedBlock)}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Observed latest block</span>
                          <strong>{formatNullable(collection.lastObservedBlock)}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Deploy block</span>
                          <strong>{formatNullable(collection.deployBlock)}</strong>
                        </div>
                        <div>
                          <span className="meta-label">Updated</span>
                          <strong>{formatDateValue(collection.updatedAt)}</strong>
                        </div>
                      </div>
                      {collection.description ? <p className="panel-copy panel-copy--lead">{collection.description}</p> : null}
                    </section>
                  </section>
                </div>
              </section>
              <MaybeCollapsibleSubsection
                title="Collection metadata"
                empty={!hasCollectionMetadataDetails}
                emptyLabel="No data"
                emptyHint="No collection metadata fields are currently populated."
              >
                {!hasCollectionMetadataDetails ? <p className="empty-state">No collection metadata fields are currently populated.</p> : null}
                <p className="panel-copy">
                  Creator fields in this section come from collection metadata, not from on-chain ownership/admin rights.
                </p>
                <dl className="detail-list detail-list--split">
                  <div>
                    <dt>External URL</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open external", href: collection.externalUrl }])}</dd>
                  </div>
                  <div>
                    <dt>Metadata creator name</dt>
                    <dd>{formatNullable(collection.creatorName)}</dd>
                  </div>
                  <div>
                    <dt>Metadata creator address</dt>
                    <dd>{collection.creatorAddress ? <span className="address-copy">{collection.creatorAddress}</span> : "-"}</dd>
                  </div>
                  <div>
                    <dt>Collection metadata hash</dt>
                    <dd>{formatNullable(collection.collectionMetadataHash)}</dd>
                  </div>
                  <div>
                    <dt>Last metadata fetch</dt>
                    <dd>{formatDateValue(collection.lastCollectionMetadataFetchAt)}</dd>
                  </div>
                  <div>
                    <dt>Metadata error</dt>
                    <dd className="detail-copy detail-copy--quiet">{formatOperatorMetadataError(collection.lastCollectionMetadataError)}</dd>
                  </div>
                  <div>
                    <dt>Collection image</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open collection image", href: toBrowserSafeDetailUrl(collection.imageOriginalUrl) }])}</dd>
                  </div>
                  <div>
                    <dt>Banner image</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open banner image", href: toBrowserSafeDetailUrl(collection.bannerImageOriginalUrl) }])}</dd>
                  </div>
                  <div>
                    <dt>Featured image</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open featured image", href: toBrowserSafeDetailUrl(collection.featuredImageOriginalUrl) }])}</dd>
                  </div>
                  <div>
                    <dt>Animation</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open animation", href: toBrowserSafeDetailUrl(collection.animationOriginalUrl) }])}</dd>
                  </div>
                  <div>
                    <dt>Audio</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open audio", href: toBrowserSafeDetailUrl(collection.audioOriginalUrl) }])}</dd>
                  </div>
                  <div>
                    <dt>Interactive</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open interactive", href: toBrowserSafeDetailUrl(collection.interactiveOriginalUrl) }])}</dd>
                  </div>
                </dl>
              </MaybeCollapsibleSubsection>
              <MaybeCollapsibleSubsection
                title="Contract signals"
                empty={!hasContractSignals}
                emptyLabel="No data"
                emptyHint="No contract-level signals are currently stored."
              >
                {!hasContractSignals ? <p className="empty-state">No contract-level signals are currently stored.</p> : null}
                <p className="panel-copy">
                  Contract owner and royalty values are read from on-chain contract interfaces and can differ from metadata creator fields.
                </p>
                <dl className="detail-list detail-list--split">
                  <div>
                    <dt>Contract owner (admin)</dt>
                    <dd>{collection.contractOwnerAddress ? <span className="address-copy">{collection.contractOwnerAddress}</span> : "-"}</dd>
                  </div>
                  <div>
                    <dt>Royalty recipient</dt>
                    <dd>
                      {collection.royaltyRecipientAddress ? <span className="address-copy">{collection.royaltyRecipientAddress}</span> : "-"}
                    </dd>
                  </div>
                  <div>
                    <dt>Royalty</dt>
                    <dd>{collection.royaltyBasisPoints != null ? `${collection.royaltyBasisPoints} bps` : "-"}</dd>
                  </div>
                  <div>
                    <dt>Contract URI raw</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open raw contract URI", href: collection.contractUriRaw }])}</dd>
                  </div>
                  <div>
                    <dt>Contract URI resolved</dt>
                    <dd>{renderDetailActionLinks([{ label: "Open resolved contract URI", href: collection.contractUriResolved }])}</dd>
                  </div>
                </dl>
              </MaybeCollapsibleSubsection>
              {collection.collectionMetadataPayload ? (
                <section className="subsection-card">
                  <h3>Collection metadata payload</h3>
                  <JsonPanel title="Collection metadata JSON" data={collection.collectionMetadataPayload} compact />
                </section>
              ) : (
                <MaybeCollapsibleSubsection
                  title="Collection metadata payload"
                  empty
                  emptyLabel="Unavailable"
                  emptyHint="No collection metadata payload is currently stored."
                >
                  <p className="empty-state empty-state--metadata">{collectionMetadataPayloadMessage}</p>
                </MaybeCollapsibleSubsection>
              )}
              <section className="subsection-card">
                <h3>Collection tokens</h3>
                {collectionTokenCards.length > 0 ? (
                  <ProgressiveCardGrid
                    className="asset-grid collection-token-grid"
                    initialCount={3}
                    increment={3}
                    buttonLabel="Load more tokens"
                    remainingLabel="more known tokens"
                  >
                    {collectionTokenCards.map((collectionToken) =>
                      renderCollectionTokenCard({
                        collectionToken,
                        collectionStandard: collection.standard
                      })
                    )}
                  </ProgressiveCardGrid>
                ) : (
                  <p className="empty-state">No currently confirmed tokens are available for this collection.</p>
                )}
              </section>
            </div>
          ) : (
            <p className="empty-state">No collection document is currently stored for this contract.</p>
          )}
        </section>
      ) : null}

      {activeView === "jobs" ? (
        <section className="panel panel--result">
          <h2>Jobs</h2>
          {jobRecords.length > 0 ? (
            <JobCardGrid jobs={jobRecords} />
          ) : (
            <p className="empty-state">No job documents are currently stored for this identity.</p>
          )}
        </section>
      ) : null}

      {activeView === "raw" ? (
        <section className="panel panel--result">
          <h2>Raw JSON</h2>
          <div className="result-stack">
            {collectionRecord || tokenRecord ? (
              <section className="subsection-card">
                <h3>Primary documents</h3>
                <div className="result-stack">
                  {collectionRecord ? <JsonPanel title="Collection document" data={collectionRecord} compact /> : null}
                  {tokenRecord ? <JsonPanel title="Token document" data={tokenRecord} compact /> : null}
                </div>
              </section>
            ) : null}
            {erc721OwnershipRecord || erc1155BalanceRecords.length > 0 ? (
              <section className="subsection-card">
                <h3>Ownership state</h3>
                <div className="result-stack">
                  {erc721OwnershipRecord ? (
                    <JsonPanel title="ERC-721 ownership document" data={erc721OwnershipRecord} compact />
                  ) : null}
                  {erc1155BalanceRecords.length > 0 ? (
                    <JsonPanel title="ERC-1155 balance documents" data={erc1155BalanceRecords} compact />
                  ) : null}
                </div>
              </section>
            ) : null}
            {mediaAssetRecords.length > 0 || metadataVersionRecords.length > 0 ? (
              <section className="subsection-card">
                <h3>Media and metadata</h3>
                <div className="result-stack">
                  {mediaAssetRecords.length > 0 ? (
                    <JsonPanel title="Media asset documents" data={mediaAssetRecords} compact />
                  ) : null}
                  {metadataVersionRecords.length > 0 ? (
                    <JsonPanel title="Metadata version documents" data={metadataVersionRecords} compact />
                  ) : null}
                </div>
              </section>
            ) : null}
            {jobRecords.length > 0 ? (
              <section className="subsection-card">
                <h3>Jobs</h3>
                <JsonPanel title="Related job documents" data={jobRecords} compact />
              </section>
            ) : null}
            {!collectionRecord &&
            !tokenRecord &&
            !erc721OwnershipRecord &&
            erc1155BalanceRecords.length === 0 &&
            mediaAssetRecords.length === 0 &&
            metadataVersionRecords.length === 0 &&
            jobRecords.length === 0 ? (
              <p className="empty-state">No raw documents are currently loaded for this identity.</p>
            ) : null}
          </div>
        </section>
      ) : null}

      {activeView === "operations" ? (
        <section className="panel panel--result panel--ops">
          <h2>Operations</h2>
          <div className="result-stack">
            {operationsHealthWarning ? <p className="empty-state">{operationsHealthWarning}</p> : null}
            <div className="metadata-grid">
              <div>
                <span className="meta-label">Active jobs</span>
                <strong>{operationsHealth.summary.activeJobsCount}</strong>
              </div>
              <div>
                <span className="meta-label">Media retries</span>
                <strong>{operationsHealth.summary.retryingMediaJobsCount}</strong>
              </div>
              <div>
                <span className="meta-label">Failed jobs</span>
                <strong>{operationsHealth.summary.failedJobsCount}</strong>
              </div>
              <div>
                <span className="meta-label">Lagging ERC-1155 collections</span>
                <strong>{operationsHealth.summary.laggingCollectionsCount}</strong>
              </div>
            </div>

            <section className="subsection-card">
              <h3>Live queue jobs</h3>
              {operationsHealth.activeJobs.length > 0 ? (
                <JobCardGrid jobs={operationsHealth.activeJobs} />
              ) : (
                <p className="empty-state">No queued or running BullMQ jobs are currently active.</p>
              )}
            </section>

            <section className="subsection-card">
              <h3>Media retries</h3>
              {operationsHealth.retryingMediaJobs.length > 0 ? (
                <JobCardGrid jobs={operationsHealth.retryingMediaJobs} />
              ) : (
                <p className="empty-state">No media jobs are currently waiting on a retry.</p>
              )}
            </section>

            <section className="subsection-card">
              <h3>Recent failures</h3>
              {operationsHealth.recentFailedJobs.length > 0 ? (
                <JobCardGrid jobs={operationsHealth.recentFailedJobs} />
              ) : (
                <p className="empty-state">No failed jobs are currently recorded.</p>
              )}
            </section>

            <section className="subsection-card">
              <h3>Indexing lag</h3>
              {operationsHealth.laggingCollections.length > 0 ? (
                <div className="table-card">
                  <table>
                    <thead>
                      <tr>
                        <th>Contract</th>
                        <th>Lag blocks</th>
                        <th>Indexed</th>
                        <th>Observed</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {operationsHealth.laggingCollections.map((collection) => (
                        <tr key={collection._id}>
                          <td className="address-copy">{collection.contractAddress}</td>
                          <td>{collection.lagBlocks}</td>
                          <td>{collection.indexedCheckpoint}</td>
                          <td>{collection.observedCheckpoint}</td>
                          <td>
                            <span className={`inline-status inline-status--${collection.syncStatus}`}>
                              {collection.syncStatus}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="empty-state">No ERC-1155 collections are currently lagging behind the observed head.</p>
              )}
            </section>
          </div>
        </section>
      ) : null}
    </main>
  );
}

function firstValue(value: string | string[] | undefined): string | undefined {
  if (Array.isArray(value)) {
    return value[0];
  }

  return value;
}

function getPollingReason(params: {
  bannerStatus: "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed" | undefined;
  collection: Awaited<ReturnType<typeof serializeEnrichedCollection>> | null;
  token: Awaited<ReturnType<typeof serializeEnrichedToken>> | null;
  jobRecords: Array<ReturnType<typeof serializeJobDocument>>;
}): string | null {
  const activeJob = params.jobRecords.find((job) => job.status === "queued" || job.status === "running");

  if (activeJob) {
    return `Job ${activeJob.type} is ${activeJob.status}. The page refreshes until MongoDB reflects the next state.`;
  }

  if (params.bannerStatus === "queued") {
    return "Discovery is queued. Waiting for token, metadata, and media documents to arrive.";
  }

  if (params.collection && ["pending", "syncing"].includes(params.collection.syncStatus)) {
    return "Collection sync is still in progress.";
  }

  if (params.token && ["pending", "stale"].includes(params.token.metadataStatus)) {
    return "Metadata is still loading from the source URI.";
  }

  if (params.token && ["pending", "processing"].includes(params.token.mediaStatus)) {
    return "Media processing is still running. Assets appear automatically once they are stored.";
  }

  return null;
}

function getTokenLookupHeading(
  status: "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed" | undefined
): string {
  switch (status) {
    case "not-found":
      return "Token not found";
    case "unresolved":
      return "Token unresolved";
    case "failed":
      return "Token refresh failed";
    case "queued":
      return "Token lookup queued";
    case "invalid":
      return "Invalid token lookup";
    default:
      return "Token not loaded";
  }
}

function getTokenLookupStateLabel(
  status: "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed" | undefined
): string {
  switch (status) {
    case "not-found":
      return "not found on-chain";
    case "unresolved":
      return "metadata unresolved";
    case "failed":
      return "refresh failed";
    case "queued":
      return "queued";
    case "invalid":
      return "invalid input";
    case "loaded":
      return "loaded";
    default:
      return "not loaded";
  }
}

function getTokenLookupMessage(params: {
  bannerStatus: "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed" | undefined;
  bannerMessage: string | undefined;
  hasCollection: boolean;
}): string {
  if (params.bannerMessage) {
    return params.bannerMessage;
  }

  if (params.bannerStatus === "not-found") {
    return params.hasCollection
      ? "The contract is known, but the requested token id could not be confirmed publicly for this collection."
      : "Neither the requested collection nor the requested token id could be confirmed publicly yet.";
  }

  if (params.bannerStatus === "unresolved") {
    return "The token may exist, but its metadata has not resolved into a usable read-model document yet.";
  }

  if (params.bannerStatus === "failed") {
    return "The refresh failed before the requested token could be confirmed from public data sources.";
  }

  if (params.bannerStatus === "queued") {
    return "The lookup is still queued. The page refreshes until collection and token state settle.";
  }

  if (params.bannerStatus === "invalid") {
    return "The requested address or token id did not pass validation.";
  }

  return "No token document is currently loaded for this requested identity.";
}

function renderCollectionTokenCard(params: {
  collectionToken: CollectionTokenCardData;
  collectionStandard: string;
}) {
  const { collectionToken, collectionStandard } = params;
  const tokenHref = buildIndexedTokenHref(collectionToken.token);
  const tokenLabel = collectionToken.name ?? `Token ${collectionToken.tokenId}`;

  return (
    <div key={collectionToken.tokenId} className="asset-card collection-token-card">
      <div className="collection-token-card__media">
        <div className="collection-token-card__media-actions">
          <a href={tokenHref} className="detail-link collection-token-card__open-link">
            Open token
          </a>
        </div>
        <MediaPreviewGallery
          token={collectionToken.token}
          mediaAssets={collectionToken.mediaAssets}
          defaultActiveIds={["image", "animation"]}
        />
      </div>
      <div className="asset-card__header collection-token-card__header">
        <div className="collection-token-card__heading">
          <a href={tokenHref} className="collection-token-card__title-link">
            <strong>{tokenLabel}</strong>
          </a>
          <span>Token {collectionToken.tokenId}</span>
        </div>
        <span className={`inline-status inline-status--${collectionToken.mediaStatus}`}>{collectionToken.mediaStatus}</span>
      </div>
      <dl className="detail-list">
        <div>
          <dt>Name</dt>
          <dd>{formatNullable(collectionToken.name)}</dd>
        </div>
        {collectionStandard === "erc1155" ? (
          <div>
            <dt>Quantity</dt>
            <dd>{formatNullable(collectionToken.supplyQuantity)}</dd>
          </div>
        ) : null}
        <div>
          <dt>Media status</dt>
          <dd>{collectionToken.mediaStatus}</dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>{formatDateValue(collectionToken.updatedAt)}</dd>
        </div>
      </dl>
      <div className="collection-token-card__footer">
        <a href={tokenHref} className="detail-link">
          Inspect token details
        </a>
      </div>
    </div>
  );
}

function buildIndexedTokenHref(
  token: Pick<ReturnType<typeof serializeTokenDocument>, "chainId" | "contractAddress" | "tokenId">
): string {
  return buildHomeHref({
    chainId: token.chainId,
    contractAddress: token.contractAddress,
    tokenId: token.tokenId,
    view: "nft",
    status: "loaded",
    message: "Loaded directly from the current read model."
  });
}

async function buildTokenCardData(database: ReturnType<typeof getWebMongoDatabase>, tokens: TokenDocument[]): Promise<CollectionTokenCardData[]> {
  if (tokens.length === 0) {
    return [];
  }

  const serializedTokens = tokens.map(serializeTokenDocument);
  const mediaAssets = await findMediaAssetsByIds({
    database,
    assetIds: serializedTokens
      .flatMap((token) => [token.imageAssetId, token.animationAssetId, token.audioAssetId])
      .filter((assetId): assetId is string => Boolean(assetId))
      .map((assetId) => new ObjectId(assetId))
  });
  const mediaAssetsById = new Map(
    mediaAssets.map((mediaAsset) => [mediaAsset._id.toHexString(), serializeMediaAssetDocument(mediaAsset)])
  );

  return serializedTokens.map((token) => ({
    token,
    tokenId: token.tokenId,
    name: token.name,
    supplyQuantity: token.supplyQuantity,
    mediaStatus: token.mediaStatus,
    updatedAt: token.updatedAt,
    mediaAssets: [token.imageAssetId, token.animationAssetId, token.audioAssetId].flatMap((assetId) => {
      if (!assetId) {
        return [];
      }

      const asset = mediaAssetsById.get(assetId);
      return asset ? [asset] : [];
    })
  }));
}

function formatCollectionHolderSummary(params: {
  collection: {
    standard: string;
    holderCount: number | null | undefined;
    lastIndexedBlock: number | null | undefined;
  };
  collectionTokenCards: CollectionTokenCardData[];
}): string {
  if (params.collection.lastIndexedBlock == null && params.collection.holderCount === 0) {
    return "not indexed yet";
  }

  if (
    params.collection.standard === "erc1155" &&
    params.collection.holderCount === 0 &&
    params.collectionTokenCards.some(
      (collectionToken) => collectionToken.supplyQuantity !== null && collectionToken.supplyQuantity !== "0"
    )
  ) {
    return "snapshot unavailable";
  }

  return formatNullable(params.collection.holderCount);
}

function formatOperatorMetadataError(value: string | null | undefined): string {
  if (!value?.trim()) {
    return "-";
  }

  const normalized = value.trim();

  if (/1 MB safety limit/i.test(normalized)) {
    return "Payload is larger than the current ingest limit.";
  }

  if (/status 404/i.test(normalized)) {
    return "Resolved source returned 404.";
  }

  if (/status 401|status 403/i.test(normalized)) {
    return "Resolved source denied access.";
  }

  if (/status 429/i.test(normalized)) {
    return "Resolved source is rate-limiting requests.";
  }

  if (/status 5\d\d/i.test(normalized)) {
    return "Resolved source is currently failing upstream.";
  }

  if (/timed out|aborted/i.test(normalized)) {
    return "Fetch timed out before the source responded.";
  }

  if (/unsupported .*protocol/i.test(normalized)) {
    return "Source uses an unsupported URI scheme.";
  }

  if (/blocked private|loopback|link-local/i.test(normalized)) {
    return "Source points to a blocked internal address.";
  }

  if (/could not be resolved/i.test(normalized)) {
    return "Source host could not be resolved.";
  }

  if (/no metadata uri candidates/i.test(normalized)) {
    return "No usable metadata URL is available yet.";
  }

  return "Source could not be loaded from the current metadata URL.";
}

function formatTokenCreatorSource(source: string | null | undefined): string {
  if (!source?.trim()) {
    return "-";
  }

  return source.startsWith("metadata") ? `metadata path: ${source}` : source;
}

function formatNullable(value: unknown): string {
  if (value === null || value === undefined) {
    return "-";
  }

  if (typeof value === "string") {
    return value.trim().length > 0 ? value : "-";
  }

  return String(value);
}

function formatDateValue(value: unknown): string {
  if (!value) {
    return "-";
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "string") {
    const parsedDate = new Date(value);
    return Number.isNaN(parsedDate.getTime()) ? value : parsedDate.toISOString();
  }

  return String(value);
}

function formatDimensions(width: unknown, height: unknown): string {
  const normalizedWidth = formatNullable(width);
  const normalizedHeight = formatNullable(height);

  if (normalizedWidth === "-" && normalizedHeight === "-") {
    return "-";
  }

  return `${normalizedWidth} x ${normalizedHeight}`;
}

function sumErc1155Balances(records: Array<ReturnType<typeof serializeErc1155BalanceDocument>>): string {
  const total = records.reduce((sum, record) => {
    try {
      return sum + BigInt(record.balance);
    } catch {
      return sum;
    }
  }, 0n);

  return total.toString();
}

function inferUriMode(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }

  if (value.startsWith("ipfs://")) {
    return "ipfs";
  }

  if (value.startsWith("ipns://")) {
    return "ipns";
  }

  if (value.startsWith("ar://")) {
    return "arweave";
  }

  if (value.startsWith("data:")) {
    return "data";
  }

  if (/^https?:\/\//i.test(value)) {
    return "http";
  }

  return "unknown";
}

function renderDetailActionLinks(
  actions: Array<{ label: string; href: string | null | undefined }>
): React.JSX.Element | string {
  const availableActions = actions.filter((action): action is { label: string; href: string } => Boolean(action.href));

  if (availableActions.length === 0) {
    return "-";
  }

  return (
    <div className="detail-action-links">
      {availableActions.map((action) => (
        <a
          key={`${action.label}-${action.href}`}
          href={action.href}
          target="_blank"
          rel="noreferrer"
          className="detail-link"
        >
          {action.label}
        </a>
      ))}
    </div>
  );
}

function hasMeaningfulValue(value: unknown): boolean {
  if (value === null || value === undefined) {
    return false;
  }

  if (typeof value === "string") {
    return value.trim().length > 0;
  }

  if (value instanceof Date) {
    return !Number.isNaN(value.getTime());
  }

  return true;
}

function MaybeCollapsibleSubsection(props: {
  title: string;
  empty: boolean;
  emptyLabel: string;
  emptyHint: string;
  children: React.ReactNode;
}): React.JSX.Element {
  if (!props.empty) {
    return (
      <section className="subsection-card">
        <h3>{props.title}</h3>
        {props.children}
      </section>
    );
  }

  return (
    <details className="subsection-card subsection-card--collapsible subsection-card--empty">
      <summary className="subsection-card__summary">
        <div className="subsection-card__summary-copy">
          <h3>{props.title}</h3>
          <p>{props.emptyHint}</p>
        </div>
        <span className="subsection-card__status">{props.emptyLabel}</span>
      </summary>
      <div className="subsection-card__body">{props.children}</div>
    </details>
  );
}

function ViewMenu(props: {
  activeView: HomeView;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  status: "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed" | undefined;
  message: string | undefined;
}) {
  const items: Array<{ view: HomeView; label: string; description: string }> = [
    {
      view: "nft",
      label: "NFT info",
      description: "Token media, attributes, ownership, and metadata details"
    },
    {
      view: "collection",
      label: "Collection",
      description: "Collection metadata, contract signals, and indexed tokens"
    },
    {
      view: "jobs",
      label: "Jobs",
      description: "Refresh and reindex jobs for the current token or collection"
    },
    {
      view: "raw",
      label: "Raw JSON",
      description: "Stored documents exactly as they are loaded from MongoDB"
    },
    {
      view: "operations",
      label: "Operations",
      description: "Queue health, retries, failures, and indexing lag"
    }
  ];

  return (
    <nav className="view-menu" aria-label="Discover views">
      {items.map((item) => {
        const isActive = item.view === props.activeView;

        return (
          <a
            key={item.view}
            href={buildHomeHref({
              chainId: props.chainId,
              contractAddress: props.contractAddress,
              ...(props.tokenId ? { tokenId: props.tokenId } : {}),
              ...(props.status ? { status: props.status } : {}),
              ...(props.message ? { message: props.message } : {}),
              view: item.view
            })}
            className={`view-tab${isActive ? " view-tab--active" : ""}`}
            aria-current={isActive ? "page" : undefined}
          >
            <strong>{item.label}</strong>
            <span>{item.description}</span>
          </a>
        );
      })}
    </nav>
  );
}

function JobCardGrid(props: { jobs: Array<ReturnType<typeof serializeJobDocument>> }) {
  return (
    <div className="job-list">
      {props.jobs.map((job) => (
        <div key={job._id} className="job-card">
          <div className="asset-card__header">
            <strong>{job.type}</strong>
            <span className={`inline-status inline-status--${job.status}`}>{job.status}</span>
          </div>
          <dl className="detail-list">
            <div>
              <dt>Queue job id</dt>
              <dd className="address-copy">{formatNullable(job.queueJobId)}</dd>
            </div>
            <div>
              <dt>Attempts</dt>
              <dd>{job.attempts}</dd>
            </div>
            <div>
              <dt>Updated</dt>
              <dd>{formatDateValue(job.updatedAt)}</dd>
            </div>
            <div>
              <dt>Last error</dt>
              <dd>{formatNullable(job.lastError)}</dd>
            </div>
          </dl>
          <JsonPanel title="Payload" data={job.payload} compact />
        </div>
      ))}
    </div>
  );
}

type MediaPreviewToken = Pick<
  ReturnType<typeof serializeTokenDocument>,
  | "name"
  | "tokenId"
  | "mediaStatus"
  | "imageOriginalUrl"
  | "animationOriginalUrl"
  | "audioOriginalUrl"
  | "interactiveOriginalUrl"
  | "interactiveMediaType"
>;

function MediaPreviewGallery(props: {
  token: MediaPreviewToken;
  mediaAssets: Array<ReturnType<typeof serializeMediaAssetDocument>>;
  defaultActiveIds?: string[];
}) {
  const { token, mediaAssets, defaultActiveIds } = props;
  const imageAsset = mediaAssets.find((asset) => asset.kind === "image") ?? null;
  const animationAsset = mediaAssets.find((asset) => asset.kind === "animation") ?? null;
  const audioAsset = mediaAssets.find((asset) => asset.kind === "audio") ?? null;
  const imageMimeType = imageAsset?.mimeType ?? null;
  const prefersOriginalImageAsset = shouldPreferOriginalImageAsset({
    mimeType: imageMimeType,
    sourceUrl: imageAsset?.sourceUrl ?? token.imageOriginalUrl ?? null
  });
  const preferredImageAssetUrl = prefersOriginalImageAsset
    ? imageAsset?.cdnUrlOriginal ?? imageAsset?.cdnUrlOptimized ?? token.imageOriginalUrl ?? null
    : imageAsset?.cdnUrlOptimized ?? imageAsset?.cdnUrlOriginal ?? token.imageOriginalUrl ?? null;

  const imageUrl =
    toBrowserSafeMediaUrl(preferredImageAssetUrl) ??
    preferredImageAssetUrl ??
    token.imageOriginalUrl ??
    null;
  const imageStatus = getDisplayMediaAssetStatus(imageAsset?.status, imageAsset?.statusDetail, {
    sourceUrl: imageAsset?.sourceUrl ?? token.imageOriginalUrl ?? null,
    renderUrl: imageUrl,
    kind: "image",
    mimeType: imageMimeType,
    hasLocalMirror: Boolean(imageAsset?.cdnUrlOriginal ?? imageAsset?.cdnUrlOptimized ?? imageAsset?.cdnUrlThumbnail)
  });
  const imageActions = buildMediaActionLinks([
    { label: "Open media", href: imageUrl },
    { label: "Open source", href: imageAsset?.sourceUrl ?? token.imageOriginalUrl }
  ]);

  const animationMimeType = animationAsset?.mimeType ?? null;
  const animationSourceUrl = animationAsset?.sourceUrl ?? token.animationOriginalUrl ?? null;
  const animationSourcePresentation = animationSourceUrl
    ? inferMediaPresentation(animationSourceUrl, animationMimeType, "animation")
    : "unknown";
  const animationUrl =
    animationSourcePresentation === "interactive"
      ? normalizeBrowserMediaUrl(animationSourceUrl) ?? animationSourceUrl
      : toBrowserSafeMediaUrl(animationAsset?.cdnUrlOriginal ?? token.animationOriginalUrl ?? null) ??
        animationAsset?.cdnUrlOriginal ??
        token.animationOriginalUrl ??
        null;
  const animationStatus = getDisplayMediaAssetStatus(animationAsset?.status, animationAsset?.statusDetail, {
    sourceUrl: animationAsset?.sourceUrl ?? token.animationOriginalUrl ?? null,
    renderUrl: animationUrl,
    kind: "animation",
    mimeType: animationMimeType,
    hasLocalMirror: Boolean(animationAsset?.cdnUrlOriginal ?? animationAsset?.cdnUrlOptimized ?? animationAsset?.cdnUrlThumbnail)
  });
  const animationPresentation = animationUrl
    ? inferMediaPresentation(animationUrl, animationMimeType, "animation")
    : "unknown";
  const animationViewUrl = animationUrl;
  const animationActions = buildMediaActionLinks([
    { label: animationPresentation === "interactive" ? "Open experience" : "Open animation", href: animationViewUrl },
    { label: "Open source", href: animationAsset?.sourceUrl ?? token.animationOriginalUrl }
  ]);

  const audioUrl =
    toBrowserSafeMediaUrl(audioAsset?.cdnUrlOriginal ?? token.audioOriginalUrl ?? null) ??
    audioAsset?.cdnUrlOriginal ??
    token.audioOriginalUrl ??
    null;
  const audioMimeType = audioAsset?.mimeType ?? null;
  const audioStatus = getDisplayMediaAssetStatus(audioAsset?.status, audioAsset?.statusDetail, {
    sourceUrl: audioAsset?.sourceUrl ?? token.audioOriginalUrl ?? null,
    renderUrl: audioUrl,
    kind: "audio",
    mimeType: audioMimeType
  });
  const audioActions = buildMediaActionLinks([
    { label: "Open audio", href: audioUrl },
    { label: "Open source", href: audioAsset?.sourceUrl ?? token.audioOriginalUrl }
  ]);

  const interactiveUrl = token.interactiveOriginalUrl ?? null;
  const interactiveType = token.interactiveMediaType ?? null;
  const interactiveActions = buildMediaActionLinks([
    { label: interactiveType === "youtube" ? "Open experience" : "Open interactive", href: interactiveUrl }
  ]);

  const items: MediaGalleryItem[] = [];

  if (imageUrl || imageActions.length > 0) {
    items.push({
      id: "image",
      label: "Image",
      status: resolveMediaSlotStatus(imageStatus, imageUrl, token.mediaStatus),
      detail: imageMimeType ?? token.imageOriginalUrl ?? null,
      actions: imageActions,
      stage: imageUrl
        ? {
            kind: "image",
            url: imageUrl,
            mimeType: imageMimeType ?? inferMimeTypeFromUrl(imageUrl, "image"),
            copy: null
          }
        : null
    });
  }

  if (animationViewUrl || animationActions.length > 0) {
    items.push({
      id: "animation",
      label: animationPresentation === "video" ? "Video" : "Animation",
      status: resolveMediaSlotStatus(animationStatus, animationViewUrl, token.mediaStatus),
      detail: animationMimeType ?? token.animationOriginalUrl ?? null,
      actions: animationActions,
      stage: animationViewUrl
        ? animationPresentation === "interactive"
          ? {
              kind: "interactive",
              url: animationViewUrl,
              mimeType: animationMimeType ?? inferMimeTypeFromUrl(animationViewUrl, "animation"),
              renderMode: "iframe",
              copy: "HTML-based animation loads inline when selected."
            }
          : animationPresentation === "image"
            ? {
                kind: "image",
                url: animationViewUrl,
                mimeType: animationMimeType ?? inferMimeTypeFromUrl(animationViewUrl, "animation"),
                copy: null
              }
            : animationPresentation === "video"
              ? {
                  kind: "video",
                  url: animationViewUrl,
                  mimeType: animationMimeType ?? inferMimeTypeFromUrl(animationViewUrl, "animation"),
                  copy: null
                }
              : animationPresentation === "audio"
                ? {
                    kind: "audio",
                    url: animationViewUrl,
                    mimeType: animationMimeType ?? inferMimeTypeFromUrl(animationViewUrl, "animation"),
                    copy: "Inline audio playback is available for this animation asset."
                  }
                : {
                    kind: "unknown",
                    url: animationViewUrl,
                    mimeType: animationMimeType ?? inferMimeTypeFromUrl(animationViewUrl, "animation"),
                    renderMode: "placeholder",
                    copy: "This animation source is available, but no inline renderer matched its format."
                  }
        : null
    });
  }

  if (audioUrl || audioActions.length > 0) {
    items.push({
      id: "audio",
      label: "Audio",
      status: resolveMediaSlotStatus(audioStatus, audioUrl, token.mediaStatus),
      detail: audioMimeType ?? token.audioOriginalUrl ?? null,
      actions: audioActions,
      stage: audioUrl
        ? {
            kind: "audio",
            url: audioUrl,
            mimeType: audioMimeType ?? inferMimeTypeFromUrl(audioUrl, "audio"),
            copy: "Inline audio playback is available for this token asset."
          }
        : null
    });
  }

  if (interactiveUrl || interactiveActions.length > 0) {
    items.push({
      id: "interactive",
      label: interactiveType === "youtube" ? "Experience" : "Interactive",
      status: interactiveUrl ? "ready" : "absent",
      detail: interactiveType ? `external ${interactiveType}` : null,
      actions: interactiveActions,
      stage: interactiveUrl
        ? interactiveType === "html"
          ? {
              kind: "interactive",
              url: interactiveUrl,
              mimeType: "text/html",
              renderMode: "iframe",
              copy: "HTML-based token content loads inline when selected."
            }
          : {
              kind: "unknown",
              url: interactiveUrl,
              mimeType: interactiveType,
              renderMode: "placeholder",
              copy:
                interactiveType === "youtube"
                  ? "This token links to an external YouTube experience. Open it in a new tab from the actions below."
                  : "This token points to an external interactive asset."
            }
        : null
    });
  }

  const resolvedDefaultActiveIds =
    animationPresentation === "interactive"
      ? ["image", ...(defaultActiveIds ?? ["animation", "image"]).filter((id) => id !== "image")]
      : defaultActiveIds;

  return (
    <InteractiveMediaPreviewGallery
      subjectName={token.name}
      subjectId={token.tokenId}
      subjectLabel="token"
      mediaStatus={token.mediaStatus}
      items={items}
      defaultActiveIds={resolvedDefaultActiveIds}
      partialMessage={
        token.mediaStatus === "partial"
          ? "Some media assets were kept as external fallbacks because not every source could be mirrored into local storage."
          : null
      }
      incompleteMessage={
        mediaAssets.length === 0 && token.mediaStatus !== "ready"
          ? "Media documents are not complete yet. The page stays in live-refresh until they appear."
          : null
      }
    />
  );
}

async function CollectionMediaPreviewGallery(props: {
  collection: Awaited<ReturnType<typeof serializeEnrichedCollection>>;
}) {
  const { collection } = props;
  const collectionImageUrl = toBrowserSafeMediaUrl(collection.imageOriginalUrl) ?? collection.imageOriginalUrl ?? null;
  const featuredImageUrl =
    toBrowserSafeMediaUrl(collection.featuredImageOriginalUrl) ?? collection.featuredImageOriginalUrl ?? null;
  const bannerImageUrl =
    toBrowserSafeMediaUrl(collection.bannerImageOriginalUrl) ?? collection.bannerImageOriginalUrl ?? null;
  const animationUrl = toBrowserSafeMediaUrl(collection.animationOriginalUrl) ?? collection.animationOriginalUrl ?? null;
  const audioUrl = toBrowserSafeMediaUrl(collection.audioOriginalUrl) ?? collection.audioOriginalUrl ?? null;
  const interactiveUrl = collection.interactiveOriginalUrl ?? null;
  const hasCollectionMedia = Boolean(collectionImageUrl || featuredImageUrl || bannerImageUrl || animationUrl || audioUrl || interactiveUrl);
  const items: MediaGalleryItem[] = [];
  const [
    collectionImageMimeType,
    featuredMimeType,
    bannerMimeType,
    animationMimeType,
    audioMimeType
  ] = await Promise.all([
    resolveDirectMediaMimeType(collectionImageUrl, "image"),
    resolveDirectMediaMimeType(featuredImageUrl, "image"),
    resolveDirectMediaMimeType(bannerImageUrl, "image"),
    resolveDirectMediaMimeType(animationUrl, "animation"),
    resolveDirectMediaMimeType(audioUrl, "audio")
  ]);

  if (collectionImageUrl || collection.imageOriginalUrl) {
    items.push({
      id: "image",
      label: "Image",
      status: collectionImageUrl ? "ready" : "absent",
      detail: collection.imageOriginalUrl,
      actions: buildMediaActionLinks([
        { label: "Open media", href: collectionImageUrl },
        { label: "Open source", href: collection.imageOriginalUrl }
      ]),
      stage: collectionImageUrl
        ? buildDirectMediaStage({
            url: collectionImageUrl,
            mimeType: collectionImageMimeType,
            fallbackKind: "image",
            preferImageOnUnknown: true,
            audioCopy: "Inline audio playback is available for this collection media.",
            interactiveCopy: "HTML-based collection media loads inline when selected.",
            unknownCopy: "This collection media is available, but no inline renderer matched its format."
          })
        : null
    });
  }

  if (bannerImageUrl || collection.bannerImageOriginalUrl) {
    items.push({
      id: "banner",
      label: "Banner",
      status: bannerImageUrl ? "ready" : "absent",
      detail: collection.bannerImageOriginalUrl,
      actions: buildMediaActionLinks([
        { label: "Open media", href: bannerImageUrl },
        { label: "Open source", href: collection.bannerImageOriginalUrl }
      ]),
      stage: bannerImageUrl
        ? buildDirectMediaStage({
            url: bannerImageUrl,
            mimeType: bannerMimeType,
            fallbackKind: "image",
            preferImageOnUnknown: true,
            audioCopy: "Inline audio playback is available for this collection banner media.",
            interactiveCopy: "HTML-based banner media loads inline when selected.",
            unknownCopy: "Banner media uses the same inline viewer and source-link flow as token media."
          })
        : null
    });
  }

  if (featuredImageUrl || collection.featuredImageOriginalUrl) {
    items.push({
      id: "featured",
      label: "Featured",
      status: featuredImageUrl ? "ready" : "absent",
      detail: collection.featuredImageOriginalUrl,
      actions: buildMediaActionLinks([
        { label: "Open media", href: featuredImageUrl },
        { label: "Open source", href: collection.featuredImageOriginalUrl }
      ]),
      stage: featuredImageUrl
        ? buildDirectMediaStage({
            url: featuredImageUrl,
            mimeType: featuredMimeType,
            fallbackKind: "image",
            preferImageOnUnknown: true,
            audioCopy: "Inline audio playback is available for this featured collection media.",
            interactiveCopy: "HTML-based featured collection media loads inline when selected.",
            unknownCopy: "Featured collection media can be opened externally when no inline renderer matches its format."
          })
        : null
    });
  }

  if (animationUrl || collection.animationOriginalUrl) {
    items.push({
      id: "animation",
      label: "Animation",
      status: animationUrl ? "ready" : "absent",
      detail: collection.animationOriginalUrl,
      actions: buildMediaActionLinks([
        { label: "Open animation", href: animationUrl },
        { label: "Open source", href: collection.animationOriginalUrl }
      ]),
      stage: animationUrl
        ? buildDirectMediaStage({
            url: animationUrl,
            mimeType: animationMimeType,
            fallbackKind: "animation",
            audioCopy: "Inline audio playback is available for this collection animation asset.",
            interactiveCopy: "HTML-based collection animation loads inline when selected.",
            unknownCopy: "This collection animation is available, but no inline renderer matched its format."
          })
        : null
    });
  }

  if (audioUrl || collection.audioOriginalUrl) {
    items.push({
      id: "audio",
      label: "Audio",
      status: audioUrl ? "ready" : "absent",
      detail: collection.audioOriginalUrl,
      actions: buildMediaActionLinks([
        { label: "Open audio", href: audioUrl },
        { label: "Open source", href: collection.audioOriginalUrl }
      ]),
      stage: audioUrl
        ? {
            kind: "audio",
            url: audioUrl,
            mimeType: audioMimeType,
            copy: "Inline audio playback is available for this collection audio asset."
          }
        : null
    });
  }

  if (interactiveUrl || collection.interactiveOriginalUrl) {
    const interactiveMimeType = inferMimeTypeFromUrl(interactiveUrl, "animation");
    items.push({
      id: "interactive",
      label: "Interactive",
      status: interactiveUrl ? "ready" : "absent",
      detail: collection.interactiveOriginalUrl,
      actions: buildMediaActionLinks([{ label: "Open interactive", href: collection.interactiveOriginalUrl }]),
      stage: interactiveUrl
        ? buildDirectMediaStage({
            url: interactiveUrl,
            mimeType: interactiveMimeType,
            fallbackKind: "animation",
            audioCopy: "Inline audio playback is available for this interactive collection media.",
            interactiveCopy: "HTML-based collection media loads inline when selected.",
            unknownCopy: "This collection points to an external interactive asset."
          })
        : null
    });
  }

  return (
    <InteractiveMediaPreviewGallery
      subjectName={collection.name}
      subjectId={collection.contractAddress}
      subjectLabel="collection"
      mediaStatus={hasCollectionMedia ? "ready" : collection.syncStatus}
      items={items}
      defaultActiveIds={["animation", "image"]}
      partialMessage={null}
      incompleteMessage={
        hasCollectionMedia
          ? null
          : collection.syncStatus === "pending" || collection.syncStatus === "syncing"
            ? "Collection metadata is still syncing. Media will appear here as soon as collection media fields are populated."
            : "No collection media URLs are currently stored for this collection."
      }
    />
  );
}

async function resolveDirectMediaMimeType(
  url: string | null | undefined,
  fallbackKind: "image" | "animation" | "audio"
): Promise<string | null> {
  const inferredMimeType = inferMimeTypeFromUrl(url, fallbackKind);

  if (inferredMimeType || !url) {
    return inferredMimeType;
  }

  const probeUrl = unwrapMediaProxyUrl(url) ?? url;

  if (!/^https?:\/\//i.test(probeUrl)) {
    return null;
  }

  try {
    const response = await fetch(probeUrl, {
      method: "HEAD",
      redirect: "follow",
      signal: AbortSignal.timeout(4_000)
    });
    const contentType = response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() ?? null;

    return contentType || null;
  } catch {
    return null;
  }
}

function buildDirectMediaStage(params: {
  url: string;
  mimeType: string | null;
  fallbackKind: "image" | "animation" | "audio";
  preferImageOnUnknown?: boolean;
  audioCopy: string;
  interactiveCopy: string;
  unknownCopy: string;
}): NonNullable<MediaGalleryItem["stage"]> {
  const presentation = inferMediaPresentation(params.url, params.mimeType, params.fallbackKind);
  const inferredMimeType = params.mimeType ?? inferMimeTypeFromUrl(params.url, params.fallbackKind);
  const resolvedPresentation =
    presentation === "unknown" && params.preferImageOnUnknown ? "image" : presentation;

  if (resolvedPresentation === "image") {
    return {
      kind: "image",
      url: params.url,
      mimeType: inferredMimeType,
      copy: null
    };
  }

  if (resolvedPresentation === "video") {
    return {
      kind: "video",
      url: params.url,
      mimeType: inferredMimeType,
      copy: null
    };
  }

  if (resolvedPresentation === "audio") {
    return {
      kind: "audio",
      url: params.url,
      mimeType: inferredMimeType,
      copy: params.audioCopy
    };
  }

  if (resolvedPresentation === "interactive") {
    return {
      kind: "interactive",
      url: params.url,
      mimeType: inferredMimeType,
      renderMode: "iframe",
      copy: params.interactiveCopy
    };
  }

  return {
    kind: "unknown",
    url: params.url,
    mimeType: inferredMimeType,
    renderMode: "placeholder",
    copy: params.unknownCopy
  };
}

function resolveMediaSlotStatus(
  explicitStatus: string | undefined,
  url: string | null | undefined,
  mediaStatus: string
): string {
  if (explicitStatus) {
    return explicitStatus;
  }

  if (url) {
    return mediaStatus;
  }

  return mediaStatus === "pending" || mediaStatus === "processing" ? mediaStatus : "absent";
}

function getDisplayMediaAssetStatus(
  status: string | null | undefined,
  statusDetail: string | null | undefined,
  context?: {
    sourceUrl?: string | null;
    renderUrl?: string | null;
    kind?: "image" | "animation" | "audio";
    mimeType?: string | null;
    hasLocalMirror?: boolean;
  }
): string | undefined {
  if (!status) {
    return undefined;
  }

  if (status === "failed" && statusDetail?.startsWith("External fallback retained")) {
    return "external-fallback";
  }

  if (
    status === "failed" &&
    context?.hasLocalMirror === false &&
    context?.sourceUrl &&
    context.kind &&
    inferMediaPresentation(context.sourceUrl, context.mimeType ?? null, context.kind) !== "unknown"
  ) {
    return "external-fallback";
  }

  return status;
}

function formatStatusLabel(status: string): string {
  return status.replace(/-/g, " ");
}

function buildMediaActionLinks(
  candidates: Array<{ label: string; href: string | null | undefined }>
): MediaActionLink[] {
  const actions: MediaActionLink[] = [];
  const seenHrefs = new Set<string>();

  for (const candidate of candidates) {
    if (!candidate.href || seenHrefs.has(candidate.href)) {
      continue;
    }

    seenHrefs.add(candidate.href);
    actions.push({
      label: candidate.label,
      href: candidate.href
    });
  }

  return actions;
}

function inferMediaPresentation(
  url: string,
  mimeType: string | null,
  fallbackKind: "image" | "animation" | "audio"
): "image" | "video" | "audio" | "interactive" | "unknown" {
  const normalizedMimeType = mimeType?.split(";")[0]?.trim().toLowerCase() ?? null;
  const detectionUrl = unwrapMediaProxyUrl(url) ?? url;
  const normalizedUrl = detectionUrl.toLowerCase();

  if (normalizedMimeType?.startsWith("image/")) {
    return "image";
  }

  if (normalizedMimeType?.startsWith("video/")) {
    return "video";
  }

  if (normalizedMimeType?.startsWith("audio/")) {
    return "audio";
  }

  if (normalizedMimeType?.includes("html")) {
    return "interactive";
  }

  if (normalizedUrl.startsWith("data:image/")) {
    return "image";
  }

  if (normalizedUrl.startsWith("data:video/")) {
    return "video";
  }

  if (normalizedUrl.startsWith("data:audio/")) {
    return "audio";
  }

  if (normalizedUrl.startsWith("data:text/html")) {
    return "interactive";
  }

  if (/\.(png|jpe?g|webp|gif|bmp|svg)(?:[?#].*)?$/i.test(normalizedUrl)) {
    return "image";
  }

  if (/\.(mp4|webm|mov|m4v|ogv)(?:[?#].*)?$/i.test(normalizedUrl)) {
    return "video";
  }

  if (/\.(mp3|wav|aac|flac|oga|ogg|m4a)(?:[?#].*)?$/i.test(normalizedUrl)) {
    return "audio";
  }

  if (/\.(html?|xhtml)(?:[?#].*)?$/i.test(normalizedUrl)) {
    return "interactive";
  }

  return fallbackKind === "audio" ? "audio" : "unknown";
}

function inferMimeTypeFromUrl(
  url: string | null | undefined,
  fallbackKind: "image" | "animation" | "audio"
): string | null {
  if (!url) {
    return null;
  }

  const dataUrlMatch = url.match(/^data:([^;,]+)[;,]/i);

  if (dataUrlMatch?.[1]) {
    return dataUrlMatch[1].toLowerCase();
  }

  const detectionUrl = unwrapMediaProxyUrl(url) ?? url;
  const normalizedUrl = detectionUrl.toLowerCase();
  const extensionMap: Record<string, string> = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
    ".svg": "image/svg+xml",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
    ".m4v": "video/x-m4v",
    ".ogv": "video/ogg",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".aac": "audio/aac",
    ".flac": "audio/flac",
    ".ogg": fallbackKind === "audio" ? "audio/ogg" : "video/ogg",
    ".oga": "audio/ogg",
    ".m4a": "audio/mp4",
    ".html": "text/html",
    ".htm": "text/html"
  };

  for (const [extension, mimeType] of Object.entries(extensionMap)) {
    if (normalizedUrl.match(new RegExp(`${extension.replace(".", "\\.")}(?:[?#].*)?$`, "i"))) {
      return mimeType;
    }
  }

  return null;
}

function shouldPreferOriginalImageAsset(params: {
  mimeType: string | null;
  sourceUrl: string | null;
}): boolean {
  const normalizedMimeType = params.mimeType?.split(";")[0]?.trim().toLowerCase() ?? null;

  if (normalizedMimeType === "image/svg+xml") {
    return true;
  }

  const inferredMimeType = inferMimeTypeFromUrl(params.sourceUrl, "image");

  return inferredMimeType === "image/svg+xml";
}

function summarizeUrlForDisplay(url: string): string {
  const proxiedMediaTarget = unwrapMediaProxyUrl(url);

  if (proxiedMediaTarget) {
    return summarizeUrlForDisplay(proxiedMediaTarget);
  }

  if (!url.startsWith("data:")) {
    return truncateMiddle(url, 112);
  }

  const [metadataSection = "data:"] = url.split(",", 1);
  return `${metadataSection.slice(0, 72)}...`;
}

function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  const visibleEdgeLength = Math.max(20, Math.floor((maxLength - 3) / 2));
  return `${value.slice(0, visibleEdgeLength)}...${value.slice(-visibleEdgeLength)}`;
}

function toBrowserSafeMediaUrl(url: string | null | undefined): string | null {
  if (!url) {
    return null;
  }

  const normalizedUrl = normalizeBrowserMediaUrl(url);

  if (!normalizedUrl) {
    return null;
  }

  if (!shouldProxyMediaUrl(normalizedUrl)) {
    return normalizedUrl;
  }

  return `/api/media?url=${encodeURIComponent(normalizedUrl)}`;
}

function toBrowserSafeDetailUrl(url: string | null | undefined): string | null {
  return toBrowserSafeMediaUrl(url) ?? normalizeBrowserMediaUrl(url ?? null);
}

function normalizeBrowserMediaUrl(url: string | null | undefined): string | null {
  if (!url) {
    return null;
  }

  if (url.startsWith("ipfs://")) {
    const remainder = url.slice("ipfs://".length).replace(/^\/+/, "");

    if (!remainder) {
      return url;
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

  if (url.startsWith("ar://")) {
    return url.replace("ar://", "https://arweave.net/");
  }

  return url;
}

function looksLikeIpfsCid(value: string): boolean {
  return /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(value) || /^b[a-z2-7]{20,}$/i.test(value);
}

function unwrapMediaProxyUrl(url: string): string | null {
  try {
    const parsedUrl = url.startsWith("http://") || url.startsWith("https://")
      ? new URL(url)
      : new URL(url, "http://localhost");

    if (parsedUrl.pathname !== "/api/media") {
      return null;
    }

    return parsedUrl.searchParams.get("url");
  } catch {
    return null;
  }
}

function resolvePreferredInteractiveMediaUrl(...candidates: Array<string | null | undefined>): string | null {
  const normalizedCandidates = candidates.filter((candidate): candidate is string => Boolean(candidate));

  const externalCandidate = normalizedCandidates.find((candidate) => !isLoopbackMediaUrl(candidate));

  return externalCandidate ?? normalizedCandidates[0] ?? null;
}

function isLoopbackMediaUrl(url: string): boolean {
  const unwrappedUrl = unwrapMediaProxyUrl(url) ?? url;

  try {
    const parsedUrl = new URL(unwrappedUrl);
    return isLoopbackHostname(parsedUrl.hostname);
  } catch {
    return false;
  }
}

function isLoopbackHostname(hostname: string): boolean {
  const normalizedHost = hostname.toLowerCase();

  return normalizedHost === "localhost" || normalizedHost === "127.0.0.1" || normalizedHost === "::1";
}

function shouldProxyMediaUrl(url: string): boolean {
  try {
    const parsedUrl = new URL(url);
    const configuredMediaBase = new URL(webRuntimeConfig.mediaPublicBaseUrl);
    const normalizedMediaBasePath = configuredMediaBase.pathname.endsWith("/")
      ? configuredMediaBase.pathname
      : `${configuredMediaBase.pathname}/`;

    if (isLoopbackHostname(parsedUrl.hostname)) {
      return true;
    }

    if (
      parsedUrl.origin === configuredMediaBase.origin &&
      (parsedUrl.pathname === configuredMediaBase.pathname ||
        parsedUrl.pathname.startsWith(normalizedMediaBasePath))
    ) {
      return true;
    }

    return false;
  } catch {
    return false;
  }
}

function buildHomeHref(params: {
  chainId?: number;
  contractAddress?: string;
  tokenId?: string;
  status?: string;
  message?: string;
  view?: HomeView;
}): string {
  const searchParams = new URLSearchParams();

  if (params.chainId) {
    searchParams.set("chainId", String(params.chainId));
  }

  if (params.contractAddress) {
    searchParams.set("contractAddress", params.contractAddress);
  }

  if (params.tokenId) {
    searchParams.set("tokenId", params.tokenId);
  }

  if (params.view) {
    searchParams.set("view", params.view);
  }

  if (params.status) {
    searchParams.set("status", params.status);
  }

  if (params.message) {
    searchParams.set("message", params.message);
  }

  const query = searchParams.toString();
  return query ? `/?${query}` : "/";
}

function JsonPanel(props: { title: string; data: unknown; compact?: boolean }) {
  return (
    <details className={`json-panel${props.compact ? " json-panel--compact" : ""}`} open={!props.compact}>
      <summary>{props.title}</summary>
      <pre>{JSON.stringify(props.data, jsonReplacer, 2)}</pre>
    </details>
  );
}

function jsonReplacer(_key: string, value: unknown) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  return value;
}

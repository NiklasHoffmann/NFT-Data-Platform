"use client";

import { useEffect, useMemo, useRef, useState } from "react";

export type MediaActionLink = {
  label: string;
  href: string;
};

export type MediaGalleryItem = {
  id: string;
  label: string;
  status: string;
  detail: string | null;
  actions: MediaActionLink[];
  stage: {
    kind: "image" | "video" | "audio" | "interactive" | "unknown";
    url: string;
    mimeType: string | null;
    renderMode?: "iframe" | "placeholder";
    copy?: string | null;
  } | null;
};

export function InteractiveMediaPreviewGallery(props: {
  subjectName: string | null;
  subjectId: string;
  subjectLabel: string;
  mediaStatus: string;
  items: MediaGalleryItem[];
  defaultActiveIds?: string[] | undefined;
  partialMessage: string | null;
  incompleteMessage: string | null;
}) {
  const availableItems = useMemo(
    () => props.items.filter((item) => item.stage !== null || item.actions.length > 0),
    [props.items]
  );
  const defaultActiveId = useMemo(
    () => getDefaultActiveId(availableItems, props.defaultActiveIds),
    [availableItems, props.defaultActiveIds]
  );
  const [activeId, setActiveId] = useState<string | null>(defaultActiveId);

  useEffect(() => {
    setActiveId((current) => {
      if (current && availableItems.some((item) => item.id === current)) {
        return current;
      }

      return defaultActiveId;
    });
  }, [availableItems, defaultActiveId]);

  const activeItem = availableItems.find((item) => item.id === activeId) ?? availableItems[0] ?? null;
  const selectableItems = activeItem
    ? availableItems.filter((item) => item.id !== activeItem.id)
    : availableItems;
  const activeStageKind = activeItem?.stage?.kind ?? null;
  const usesFlushVisualFrame = activeStageKind === "image" || activeStageKind === "video" || activeStageKind === "interactive";

  if (!activeItem) {
    return (
      <div className="media-gallery media-gallery--switcher">
        <div className="media-placeholder media-placeholder--viewer">
          <strong>No primary media yet</strong>
          <p>
            Current media status: <span className={`inline-status inline-status--${props.mediaStatus}`}>{props.mediaStatus}</span>
          </p>
        </div>
        {props.partialMessage ? <p className="banner-copy">{props.partialMessage}</p> : null}
        {props.incompleteMessage ? <p className="banner-copy">{props.incompleteMessage}</p> : null}
      </div>
    );
  }

  return (
    <div className="media-gallery media-gallery--switcher">
      <div className="media-stage media-stage--primary">
        <div className="media-stage-card media-stage-card--primary">
          <div className={`media-stage-frame${usesFlushVisualFrame ? " media-stage-frame--visual" : ""}`}>
            <div className={`media-stage-canvas${usesFlushVisualFrame ? " media-stage-canvas--visual" : ""}`}>
              {renderStage(activeItem, props.subjectName, props.subjectId, props.subjectLabel)}
            </div>
          </div>
        </div>

        <div className="media-stage-toolbar">
          <div className="media-stage-toolbar__top">
            <div className="media-stage-toolbar__meta">
              <strong>{activeItem.label}</strong>
              <span className={`inline-status inline-status--${activeItem.status}`}>{formatStatusLabel(activeItem.status)}</span>
            </div>
            {activeItem.actions.length > 0 ? <MediaActionLinks actions={activeItem.actions} /> : null}
          </div>
          {activeItem.detail ? <p className="field-copy media-stage-toolbar__detail">{activeItem.detail}</p> : null}
        </div>
      </div>

      {selectableItems.length > 0 ? (
        <div className="media-picker" aria-label={`Other ${props.subjectLabel} media choices`}>
          {selectableItems.map((item) => {
            return (
              <button
                key={item.id}
                type="button"
                className="media-picker__button"
                onClick={() => setActiveId(item.id)}
                aria-label={`Show ${item.label}`}
                title={item.detail ?? item.label}
              >
                <strong>{item.label}</strong>
                <span className={`inline-status inline-status--${item.status}`}>{formatStatusLabel(item.status)}</span>
              </button>
            );
          })}
        </div>
      ) : null}

      {props.partialMessage ? <p className="banner-copy">{props.partialMessage}</p> : null}
      {props.incompleteMessage ? <p className="banner-copy">{props.incompleteMessage}</p> : null}
    </div>
  );
}

function getDefaultActiveId(items: MediaGalleryItem[], preferredIds: string[] = ["image", "animation"]): string | null {
  for (const preferredId of preferredIds) {
    const preferredItem = items.find((item) => item.id === preferredId && item.stage);

    if (preferredItem) {
      return preferredItem.id;
    }
  }

  return items.find((item) => item.stage)?.id ?? items[0]?.id ?? null;
}

function ImageStage(props: {
  item: MediaGalleryItem;
  title: string;
}) {
  const imageUrl = props.item.stage?.url ?? null;
  const [loadState, setLoadState] = useState<"loading" | "loaded" | "error">("loading");
  const previousImageUrlRef = useRef<string | null>(imageUrl);
  const imageElementRef = useRef<HTMLImageElement | null>(null);

  useEffect(() => {
    if (previousImageUrlRef.current !== imageUrl) {
      previousImageUrlRef.current = imageUrl;
      setLoadState("loading");
    }
  }, [imageUrl]);

  useEffect(() => {
    const imageElement = imageElementRef.current;

    if (!imageElement?.complete) {
      return;
    }

    setLoadState(imageElement.naturalWidth > 0 ? "loaded" : "error");
  }, [imageUrl]);

  if (!props.item.stage || props.item.stage.kind !== "image") {
    return null;
  }

  if (loadState === "error") {
    const reason = props.item.stage.mimeType && !props.item.stage.mimeType.startsWith("image/")
      ? `The source reported ${props.item.stage.mimeType}, which could not be rendered as an inline image.`
      : "The browser could not load this image inline. Use the media or source links above if the remote file is unavailable or invalid.";

    return (
      <div className="media-placeholder media-placeholder--viewer media-placeholder--failure">
        <strong>{props.item.label} could not be loaded</strong>
        <p>{reason}</p>
      </div>
    );
  }

  return (
    <div className={`media-stage-media media-stage-media--image ${loadState === "loaded" ? "is-loaded" : "is-loading"}`} aria-busy={loadState === "loading"}>
      {loadState === "loaded" ? (
        <img
          className="token-image token-image--backdrop"
          src={props.item.stage.url}
          alt=""
          aria-hidden
          loading="eager"
        />
      ) : null}
      <img
        ref={imageElementRef}
        className={`token-image token-image--primary ${loadState === "loaded" ? "" : "token-image--pending"}`.trim()}
        src={props.item.stage.url}
        alt={props.title}
        loading={props.item.id === "image" || props.item.id === "animation" ? "eager" : "lazy"}
        onLoad={() => setLoadState("loaded")}
        onError={() => setLoadState("error")}
      />
      {loadState === "loading" ? (
        <div className="media-stage-loading" role="status" aria-live="polite">
          <span className="media-stage-spinner" aria-hidden="true" />
          <p>Loading image preview...</p>
        </div>
      ) : null}
    </div>
  );
}

function UnknownStage(props: {
  item: MediaGalleryItem;
  title: string;
}) {
  const stage = props.item.stage;
  const [resolution, setResolution] = useState<"checking" | "image" | "unsupported">("checking");

  useEffect(() => {
    if (!stage?.url) {
      setResolution("unsupported");
      return;
    }

    if (stage.mimeType?.startsWith("image/")) {
      setResolution("image");
      return;
    }

    let cancelled = false;
    const imageProbe = new window.Image();
    const fallbackTimer = window.setTimeout(() => {
      if (!cancelled) {
        setResolution("unsupported");
      }
    }, 6000);

    setResolution("checking");

    imageProbe.onload = () => {
      if (!cancelled) {
        window.clearTimeout(fallbackTimer);
        setResolution("image");
      }
    };
    imageProbe.onerror = () => {
      if (!cancelled) {
        window.clearTimeout(fallbackTimer);
        setResolution("unsupported");
      }
    };
    imageProbe.src = stage.url;

    return () => {
      cancelled = true;
      window.clearTimeout(fallbackTimer);
      imageProbe.onload = null;
      imageProbe.onerror = null;
    };
  }, [stage?.mimeType, stage?.url]);

  if (!stage) {
    return null;
  }

  if (resolution === "image") {
    return <ImageStage item={{ ...props.item, stage: { ...stage, kind: "image" } }} title={props.title} />;
  }

  if (resolution === "checking") {
    return (
      <div className="media-stage-media media-stage-media--image is-loading" aria-busy="true">
        <div className="media-stage-loading" role="status" aria-live="polite">
          <span className="media-stage-spinner" aria-hidden="true" />
          <p>Checking media preview...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="media-placeholder media-placeholder--viewer">
      <strong>{props.item.label}</strong>
      <p>{stage.copy ?? "This media source can be opened externally, but no inline renderer matched its format."}</p>
    </div>
  );
}

function renderStage(item: MediaGalleryItem, subjectName: string | null, subjectId: string, subjectLabel: string) {
  const normalizedSubjectLabel = subjectLabel.trim() || "item";
  const fallbackLabel = `${normalizedSubjectLabel.charAt(0).toUpperCase()}${normalizedSubjectLabel.slice(1)} ${subjectId}`;
  const title = subjectName ?? fallbackLabel;

  if (!item.stage) {
    return (
      <div className="media-placeholder media-placeholder--viewer">
        <strong>{item.label} is not ready yet</strong>
        <p>Select another media type or open the source link when it becomes available.</p>
      </div>
    );
  }

  if (item.stage.kind === "image") {
    return <ImageStage item={item} title={title} />;
  }

  if (item.stage.kind === "video") {
    return (
      <div className="media-stage-media media-stage-media--video">
        <video className="token-video" controls preload="metadata">
          <source src={item.stage.url} type={item.stage.mimeType ?? undefined} />
        </video>
      </div>
    );
  }

  if (item.stage.kind === "audio") {
    return (
      <div className="media-placeholder media-placeholder--viewer media-placeholder--audio">
        <audio className="token-audio" controls preload="metadata">
          <source src={item.stage.url} type={item.stage.mimeType ?? undefined} />
        </audio>
        <p>{item.stage.copy ?? "Inline audio playback is available for this media asset."}</p>
      </div>
    );
  }

  if (item.stage.kind === "interactive" && item.stage.renderMode === "iframe") {
    return (
      <iframe
        className="token-iframe token-iframe--stage"
        src={item.stage.url}
        title={`${item.label} viewer for ${title}`}
        loading="lazy"
        sandbox="allow-scripts allow-forms allow-popups allow-downloads"
        referrerPolicy="no-referrer"
      />
    );
  }

  if (item.stage.kind === "unknown") {
    return <UnknownStage item={item} title={title} />;
  }

  return (
    <div className="media-placeholder media-placeholder--viewer">
      <strong>{item.label}</strong>
      <p>{item.stage.copy ?? "This media source can be opened externally, but no inline renderer matched its format."}</p>
    </div>
  );
}

function MediaActionLinks(props: { actions: MediaActionLink[] }) {
  return (
    <div className="media-action-links">
      {props.actions.map((action) => (
        <a
          key={`${action.label}-${action.href}`}
          href={action.href}
          target="_blank"
          rel="noreferrer"
          className="ghost-button media-action-link"
        >
          {action.label}
        </a>
      ))}
    </div>
  );
}

function formatStatusLabel(status: string): string {
  return status.replace(/-/g, " ");
}
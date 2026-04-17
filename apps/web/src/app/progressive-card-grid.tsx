"use client";

import { Children, type ReactNode, useMemo, useState } from "react";

export function ProgressiveCardGrid(props: {
  children: ReactNode;
  className: string;
  initialCount: number;
  increment?: number;
  buttonLabel?: string;
  remainingLabel?: string;
}) {
  const items = useMemo(() => Children.toArray(props.children), [props.children]);
  const increment = props.increment ?? props.initialCount;
  const [visibleCount, setVisibleCount] = useState(props.initialCount);
  const visibleItems = items.slice(0, visibleCount);
  const remainingCount = Math.max(0, items.length - visibleCount);

  return (
    <div className="progressive-card-grid">
      <div className={props.className}>{visibleItems}</div>
      {remainingCount > 0 ? (
        <div className="progressive-card-grid__actions">
          <button
            type="button"
            className="ghost-button progressive-card-grid__button"
            onClick={() => setVisibleCount((current) => Math.min(items.length, current + increment))}
          >
            {props.buttonLabel ?? "Load more"}
          </button>
          <span className="progressive-card-grid__count">
            {remainingCount} {props.remainingLabel ?? "more items"}
          </span>
        </div>
      ) : null}
    </div>
  );
}

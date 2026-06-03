"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

export function DiscoverLiveRefresh(props: { active: boolean; reason: string | null }) {
  const { active, reason } = props;
  const router = useRouter();
  const [refreshCount, setRefreshCount] = useState(0);
  const [isPending, startTransition] = useTransition();
  const isPendingRef = useRef(false);

  useEffect(() => {
    isPendingRef.current = isPending;
  }, [isPending]);

  useEffect(() => {
    if (!active) {
      return;
    }

    const interval = window.setInterval(() => {
      if (isPendingRef.current) return;
      startTransition(() => {
        router.refresh();
        setRefreshCount((current) => current + 1);
      });
    }, 2500);

    return () => {
      window.clearInterval(interval);
    };
  }, [active, router, startTransition]);

  if (!active) {
    return null;
  }

  return (
    <div className="live-refresh" aria-live="polite">
      <div>
        <strong>Live refresh active</strong>
        <p>{reason ?? "Waiting for MongoDB state changes and refreshed media assets."}</p>
      </div>
      <button
        type="button"
        className="ghost-button"
        onClick={() => {
          startTransition(() => {
            router.refresh();
            setRefreshCount((current) => current + 1);
          });
        }}
      >
        {isPending ? "Refreshing..." : `Refresh now${refreshCount > 0 ? ` (${refreshCount})` : ""}`}
      </button>
    </div>
  );
}
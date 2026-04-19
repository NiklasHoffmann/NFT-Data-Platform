"use client";

import { useFormStatus } from "react-dom";

export function DiscoverSubmitButton() {
  const { pending } = useFormStatus();

  return (
    <button
      type="submit"
      className={`discover-button${pending ? " discover-button--pending" : ""}`}
      disabled={pending}
      aria-disabled={pending}
      aria-busy={pending}
    >
      {pending ? <span className="discover-button__spinner" aria-hidden="true" /> : null}
      <span>{pending ? "Discovering..." : "Discover"}</span>
    </button>
  );
}
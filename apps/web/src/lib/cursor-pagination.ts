import { ObjectId } from "mongodb";

export type UpdatedAtCursor = {
  updatedAt: Date;
  id: ObjectId;
};

export function encodeUpdatedAtCursor(value: {
  _id: ObjectId | string;
  updatedAt: Date | string;
}): string {
  const payload = JSON.stringify({
    updatedAt: new Date(value.updatedAt).toISOString(),
    id: typeof value._id === "string" ? value._id : value._id.toHexString()
  });

  return Buffer.from(payload, "utf8").toString("base64url");
}

export function decodeUpdatedAtCursor(cursor: string): UpdatedAtCursor {
  let parsed: { updatedAt: string; id: string };

  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as {
      updatedAt: string;
      id: string;
    };
  } catch {
    throw new Error("Invalid cursor encoding.");
  }

  const updatedAt = new Date(parsed.updatedAt);

  if (Number.isNaN(updatedAt.getTime()) || !ObjectId.isValid(parsed.id)) {
    throw new Error("Invalid cursor payload.");
  }

  return {
    updatedAt,
    id: new ObjectId(parsed.id)
  };
}
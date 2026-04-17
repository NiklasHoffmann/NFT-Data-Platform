import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NFT Data Platform",
  description: "Read API and operator surface for ERC-721 and ERC-1155 data."
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

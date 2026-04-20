import type { Metadata } from "next";
import { Syne, DM_Sans } from "next/font/google";
import "./globals.css";
import { cn } from "@/lib/utils";
import { TooltipProvider } from "@/components/ui/tooltip";

const fontSyne = Syne({
  subsets: ["latin"],
  display: "swap",
  weight: "800",
  variable: "--font-syne",
});

const fontDMSans = DM_Sans({
  subsets: ["latin"],
  display: "swap",
  weight: ["300", "400", "500"],
  variable: "--font-dm-sans",
});

export const metadata: Metadata = {
  title: "InsightFlow",
  description: "AI-powered automated data analytics platform.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body
        className={cn(
          "min-h-screen bg-background font-sans antialiased",
          fontSyne.variable,
          fontDMSans.variable
        )}
      >
        <TooltipProvider>{children}</TooltipProvider>
      </body>
    </html>
  );
}

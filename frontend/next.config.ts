import type { NextConfig } from "next";
import path from "path";

const config: NextConfig = {
  reactStrictMode: true,
  turbopack: {
    root: path.resolve(__dirname),
  },
};

export default config;

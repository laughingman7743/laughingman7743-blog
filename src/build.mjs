import { execFileSync } from "node:child_process";
import { generateSocialImages } from "./social-images.mjs";

const args = ["build"];
if (process.env.CF_PAGES_BRANCH && process.env.CF_PAGES_BRANCH !== "main") {
  if (!process.env.CF_PAGES_URL) {
    throw new Error("CF_PAGES_URL is required for a Cloudflare preview build.");
  }
  args.push("--base-url", process.env.CF_PAGES_URL);
}

execFileSync("zola", args, { stdio: "inherit" });
await generateSocialImages();
